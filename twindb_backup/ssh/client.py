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

    """
    def __init__(self, ssh_connect_info):

        self._ssh_connect_info = ssh_connect_info

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
            shell.connect(hostname=self._ssh_connect_info.host,
                          key_filename=self._ssh_connect_info.key,
                          port=self._ssh_connect_info.port,
                          username=self._ssh_connect_info.user)
            yield shell
        except (AuthenticationException, SSHException, socket.error) as err:
            raise SshClientException(err)
        finally:
            shell.close()

    @contextmanager
    def _get_remote_handlers(self, cmd):
        """Get remote stdin, stdout and stderr handler

        :param cmd: Command for execution
        :type cmd: list
        :return: Remote stdin, stdout and stderr handler
        :rtype: tuple(generator, generator, generator)
        :raise SshClientException: if any error
        """
        cmd_str = ' '.join(cmd)
        try:
            with self._shell() as shell:
                stdin_, stdout_, stderr_ = shell.exec_command(cmd_str)
                yield stdin_, stdout_, stderr_

        except SSHException as err:
            LOG.error('Failed to execute %s', cmd_str)
            raise SshClientException(err)

    def execute(self, cmd):
        """Execute a command on a remote SSH server.

        :param cmd: Command for execution.
        :type cmd: str
        :return: Handlers of stdin, stdout and stderr
        :rtype: tuple
        :raise SshDestinationError: if any error

        """
        try:
            with self._shell() as shell:
                stdin_, stdout_, stderr_ = shell.exec_command(cmd)
                exit_code = stdout_.channel.recv_exit_status()
                if exit_code != 0:
                    LOG.error("Failed while execute command %s", cmd)
                    LOG.error(stderr_.read())
                    raise SshClientException('%s exited with code %d'
                                             % (cmd, exit_code))
                return stdin_, stdout_, stderr_

        except SSHException as err:
            LOG.error('Failed to execute %s', cmd)
            raise SshClientException(err)

    @contextmanager
    def get_remote_handlers(self, cmd):
        """Get remote stdin, stdout and stderr handler

        :param cmd: Command for execution
        :type cmd: str
        :return: Remote stdin, stdout and stderr handler
        :rtype: tuple(generator, generator, generator)
        :raise SshDestinationError: if any error
        """
        try:
            with self._shell() as shell:
                stdin_, stdout_, stderr_ = shell.exec_command(cmd)
                yield stdin_, stdout_, stderr_

        except SSHException as err:
            LOG.error('Failed to execute %s', cmd)
            raise SshClientException(err)

