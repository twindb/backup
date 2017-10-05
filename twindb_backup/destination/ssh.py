# -*- coding: utf-8 -*-
"""
Module for SSH destination.
"""
import base64
import json
import socket
from contextlib import contextmanager

from paramiko import SSHClient, AuthenticationException, SSHException
from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import SshDestinationError


class SshConnectInfo(object):  # pylint: disable=too-few-public-methods
    """Options for SSH connection"""
    def __init__(self, host='127.0.0.1', port=22, key='/root/.id_rsa',
                 user='root'):
        self.host = host
        self.port = port
        self.key = key
        self.user = user


class Ssh(BaseDestination):
    """SSH destination class"""
    def __init__(self, ssh_connect_info=SshConnectInfo(),
                 remote_path=None, hostname=socket.gethostname()):
        """
        Initializes Ssh() instance.

        :param ssh_connect_info: SSH connection info
        :type ssh_connect_info: SshConnectInfo
        :param remote_path:
        :param hostname:
        """
        super(Ssh, self).__init__()
        self.remote_path = remote_path.rstrip('/')

        self.ssh_connect_info = ssh_connect_info

        self.status_path = "{remote_path}/{hostname}/status".format(
            remote_path=self.remote_path,
            hostname=hostname
        )

    def save(self, handler, name):
        """
        Read from handler and save it on remote ssh server

        :param name: relative path to a file to store the backup copy.
        :param handler: stream with content of the backup.
        :return: exit code
        """
        remote_name = self.remote_path + '/' + name
        self._mkdirname_r(remote_name)
        cmd = ["cat - > \"%s\"" % remote_name]

        stdout, _ = self._execute_command(cmd)
        if stdout.channel.recv_exit_status():
            raise SshDestinationError('%s exited with error code %d'
                                      % (' '.join(cmd),
                                         stdout.channel.recv_exit_status()))
        return stdout.channel.recv_exit_status()

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :return: exit code
        """
        cmd = ["mkdir -p \"%s\"" % path]
        LOG.debug('Running %s', ' '.join(cmd))
        stdout, _ = self._execute_command(cmd)
        if stdout.channel.recv_exit_status():
            LOG.error('Failed to create directory %s', path)
            exit(1)
        return stdout.channel.recv_exit_status()

    def list_files(self, prefix, recursive=False):

        if recursive:
            ls_cmd = ["ls -R %s*" % prefix]
        else:
            ls_cmd = ["ls %s*" % prefix]

        with self._get_remote_stdout(ls_cmd) as cout:
            return sorted(cout.read().split())

    def find_files(self, prefix, run_type):

        cmd = [
            "find {prefix}/*/{run_type} "
            "-type f".format(prefix=prefix,
                             run_type=run_type)
        ]

        with self._get_remote_stdout(cmd) as cout:
            return sorted(cout.read().split())

    def delete(self, obj):
        cmd = ["rm %s" % obj]
        LOG.debug('Running %s', ' '.join(cmd))
        self._execute_command(cmd)

    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :return: Standard output.
        """
        cmd = ["cat %s" % path]
        return self._get_remote_stdout(cmd)

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))
        cmd = [
            "echo {raw_status} > "
            "{status_file}".format(raw_status=raw_status,
                                   status_file=self.status_path)
        ]
        stdout, _ = self._execute_command(cmd)
        if stdout.channel.recv_exit_status():
            LOG.error('Failed to write backup status')
            exit(1)
        return status

    def _read_status(self):

        if self._status_exists():
            cmd = ["cat %s" % self.status_path]
            stdout, _ = self._execute_command(cmd)
            if stdout.channel.recv_exit_status():
                LOG.error('Failed to read backup status: %d',
                          stdout.channel.recv_exit_status())
                exit(1)
            return json.loads(base64.b64decode(stdout.read()))
        else:
            return self._empty_status

    def _status_exists(self):
        cmd = ["bash -c 'if test -s %s; "
               "then echo exists; "
               "else echo not_exists; "
               "fi'" % self.status_path]

        try:
            LOG.debug('Running %r', cmd)
            stdout, _ = self._execute_command(cmd)
            output = stdout.read()
            if stdout.channel.recv_exit_status():
                LOG.error('Failed to read backup status: %d',
                          stdout.channel.recv_exit_status())
                exit(1)
            if output.strip() == 'exists':
                return True
            elif output.strip() == 'not_exists':
                return False
            else:
                raise SshDestinationError('Unrecognized response: %s' % output)

        except OSError as err:
            LOG.error('Failed to run %s: %s', " ".join(cmd), err)
            exit(1)

    def share(self, url):
        super(Ssh, self).share(url)

    def _execute_command(self, cmd):
        """Execute ssh command

        :param cmd: Command for execution
        :type cmd: list
        :return: Handlers of stdout and stderr
        :raise: SshDestinationError if any error
        """
        shell = SSHClient()
        cmd_str = ' '.join(cmd)
        try:
            shell.connect(hostname=self.ssh_connect_info.host,
                          key_filename=self.ssh_connect_info.key,
                          port=self.ssh_connect_info.port,
                          username=self.ssh_connect_info.user)
            _, stdout, stderr = shell.exec_command(cmd_str)
            return stdout, stderr
        except (AuthenticationException, SSHException, socket.error) as err:
            LOG.error("Failure execution %r : %s", cmd_str, err)
            raise SshDestinationError(err)
        finally:
            shell.close()

    @contextmanager
    def _get_remote_stdout(self, cmd):
        """Get remote stdout handler

        :param cmd: Command for execution
        :type cmd: list
        :return: Remote stdout handler
        :rtype: generator
        :raise: SshDestinationError if any error
        """
        shell = SSHClient()
        cmd_str = ' '.join(cmd)
        try:
            shell.connect(hostname=self.ssh_connect_info.host,
                          key_filename=self.ssh_connect_info.key,
                          port=self.ssh_connect_info.port,
                          username=self.ssh_connect_info.user)
            _, stdout, _ = shell.exec_command(cmd_str)
            yield stdout
        except (AuthenticationException, SSHException, socket.error) as err:
            LOG.error("Failure execution %r : %s", cmd_str, err)
            raise SshDestinationError(err)
        finally:
            shell.close()

    def _mkdirname_r(self, remote_name):
        """Create directory for a given file on the destination.
        For example, for a given file '/foo/bar/xyz' it would create
        directory '/foo/bar/'.

        :param remote_name: Full path to a file
        :type remote_name: str
        :return: exit code. 0 if success.
        :rtype: int
        """
        pass
        # self._mkdir_r(os.path.dirname(remote_name))
