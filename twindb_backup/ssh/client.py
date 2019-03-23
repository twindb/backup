"""
Module that implements SSH client.
"""
import socket
from contextlib import contextmanager

from paramiko import SSHClient, AutoAddPolicy, AuthenticationException, \
    SSHException

from twindb_backup import LOG
from twindb_backup.ssh.exceptions import SshClientException


class SshClient(object):
    """
    SSH client class. Allows to connect to a remote SSH server and execute
    commands on it.

    :param host: Destination host to connect to. Detaults to '127.0.0.1'.
    :type host: str
    :param port: Destination port to connect to. Default is 22.
    :type port: int
    :param key: SSH client key for passwordless authentication.
        Default is '/root/.id_rsa'.
    :type key: str
    :param user: SSH client username. Default is 'root'.
    :type user: str
    """
    def __init__(self,
                 host='127.0.0.1',
                 port=22,
                 key='/root/.id_rsa',
                 user='root'):

        self._host = host
        self._port = port
        self._key = key
        self._user = user

    @contextmanager
    def session(self):
        """
        Get SSH session

        :rtype: generator
        :return: SSH session
        """
        with self._shell() as client:
            transport = client.get_transport()
            session = transport.open_session()
            yield session

    @contextmanager
    def _shell(self):
        """
        Create SSHClient instance and connect to the destination host.

        :return: Connected to the remote destination host shell.
        :rtype: generator(SSHClient)
        :raise SshDestinationError: if the ssh client fails to connect.
        """
        shell = SSHClient()
        shell.set_missing_host_key_policy(AutoAddPolicy())
        try:
            shell.connect(
                hostname=self._host,
                key_filename=self._key,
                port=self._port,
                username=self._user
            )
            yield shell
        except (AuthenticationException, SSHException, socket.error) as err:
            raise SshClientException(err)
        finally:
            shell.close()

    @property
    def host(self):
        """Remote SSH host"""
        return self._host

    @property
    def user(self):
        """User for SSH connection"""
        return self._user

    @property
    def port(self):
        """TCP port for SSH connection"""
        return self._port

    def execute(self, cmd, quiet=False, background=False):
        """Execute a command on a remote SSH server.

        :param cmd: Command for execution.
        :type cmd: str
        :param quiet: if quiet is True don't print error messages
        :param background: Don't wait until the command exits.
        :type background: bool
        :return: Strings with stdout and stderr. If command is executed
            in background the method will return None.
        :rtype: tuple
        :raise SshClientException: if any error or non-zero exit code

        """
        max_chunk_size = 1024 * 1024
        try:
            with self._shell() as shell:
                if not background:
                    LOG.debug('Executing command: %s', cmd)
                    stdin_, stdout_, _ = shell.exec_command(cmd)
                    channel = stdout_.channel
                    stdin_.close()
                    channel.shutdown_write()
                    stdout_chunks = []
                    stderr_chunks = []
                    while not channel.closed \
                            or channel.recv_ready() \
                            or channel.recv_stderr_ready():
                        if channel.recv_ready():
                            stdout_chunks.append(
                                channel.recv(max_chunk_size)
                            )
                        if channel.recv_stderr_ready():
                            stderr_chunks.append(
                                channel.recv_stderr(max_chunk_size)
                            )

                    exit_code = channel.recv_exit_status()
                    if exit_code != 0:
                        if not quiet:
                            LOG.error("Failed while execute command %s", cmd)
                            LOG.error(''.join(stderr_chunks))
                        raise SshClientException('%s exited with code %d'
                                                 % (cmd, exit_code))
                    return ''.join(stdout_chunks), ''.join(stderr_chunks)
                else:
                    LOG.debug('Executing in background: %s', cmd)
                    transport = shell.get_transport()
                    channel = transport.open_session()
                    channel.exec_command(cmd)
                    LOG.debug('Ran %s in background', cmd)

        except (SSHException, IOError) as err:
            if not quiet:
                LOG.error('Failed to execute %s: %s', cmd, err)
            raise SshClientException('Failed to execute %s: %s'
                                     % (cmd, err))

    @contextmanager
    def get_remote_handlers(self, cmd):
        """
        Get remote stdin, stdout and stderr handler

        :param cmd: Command for execution
        :type cmd: str
        :return: Remote stdin, stdout and stderr handler
        :rtype: tuple(generator, generator, generator)
        :raise SshDestinationError: if any error
        """
        try:
            with self._shell() as shell:
                LOG.debug("Try to get remote handlers: %s", cmd)
                stdin_, stdout_, stderr_ = shell.exec_command(cmd)
                yield stdin_, stdout_, stderr_

        except SSHException as err:
            LOG.error('Failed to execute %s', cmd)
            raise SshClientException(err)

    def list_files(self, path, recursive=False, files_only=False):
        """
        Get list of file by prefix

        :param path: Path
        :param recursive: Recursive return list of files
        :type path: str
        :type recursive: bool
        :param files_only: Don't list directories if True. Default is False.
        :type files_only: bool
        :return: List of files
        :rtype: list
        """
        rec_cond = "" if recursive else " -maxdepth 1"
        fil_cond = " -type f" if files_only else ""

        cmd = "bash -c 'if test -d {path} ; " \
              "then find {path}{recursive}{files_only}; fi'"
        cmd = cmd.format(
            path=path,
            recursive=rec_cond,
            files_only=fil_cond
        )
        cout, cerr = self.execute(cmd)
        LOG.debug("COUT:\n%s", cout)
        LOG.debug("CERR:\n%s", cerr)

        if files_only:
            return cout.split()
        else:
            return cout.split()[1:]

    def get_text_content(self, path):
        """
        Get text content of file by path

        :param path: File path
        :type path: str
        :return: File content
        :rtype: str
        """

        cout, _ = self.execute("cat %s" % path)
        return cout

    def write_content(self, path, content):
        """
        Write content to path

        :param path: Path to file
        :param content: Content
        """
        with self.get_remote_handlers("cat - > %s" % path) \
                as (cin, _, _):
            cin.write(content)

    def write_config(self, path, cfg):
        """
        Write config to file

        :param path: Path to file
        :param cfg: Instance of ConfigParser
        """
        with self.get_remote_handlers("cat - > %s" % path) \
                as (cin, _, _):
            cfg.write(cin)
