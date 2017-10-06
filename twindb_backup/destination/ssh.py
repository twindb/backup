# -*- coding: utf-8 -*-
"""
Module for SSH destination.
"""
import base64
import json
import socket
from contextlib import contextmanager

import os
from paramiko import SSHClient, AuthenticationException, SSHException, \
    AutoAddPolicy
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
    """
    SSH destination class

    :param ssh_connect_info: SSH connection info
    :type ssh_connect_info: SshConnectInfo
    :param remote_path: Path to store backup
    :param hostname: Hostname
    """
    def __init__(self, ssh_connect_info=SshConnectInfo(),
                 remote_path=None, hostname=socket.gethostname()):
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
        """
        remote_name = self.remote_path + '/' + name
        self._mkdirname_r(remote_name)
        cmd = ["cat - > \"%s\"" % remote_name]

        self.execute_command(cmd)

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :type path: str
        """
        cmd = ["mkdir -p \"%s\"" % path]
        LOG.debug('Running %s', ' '.join(cmd))
        self.execute_command(cmd)

    def list_files(self, prefix, recursive=False):
        """
        Get list of file by prefix

        :param prefix: Path
        :param recursive: Recursive return list of files
        :type prefix: str
        :type recursive: bool
        :return: List of files
        :rtype: list
        """
        if recursive:
            ls_cmd = ["ls -R %s*" % prefix]
        else:
            ls_cmd = ["ls %s*" % prefix]

        with self._get_remote_stdout(ls_cmd) as cout:
            return sorted(cout.read().split())

    def find_files(self, prefix, run_type):
        """
        Find files by prefix

        :param prefix: Path
        :param run_type: Run type for search
        :type prefix: str
        :type run_type: str
        :return: List of files
        :rtype: list
        """
        cmd = [
            "find {prefix}/*/{run_type} "
            "-type f".format(prefix=prefix,
                             run_type=run_type)
        ]

        with self._get_remote_stdout(cmd) as cout:
            return sorted(cout.read().split())

    def delete(self, obj):
        """
        Delete file by path

        :param obj: path to a remote file.
        """
        cmd = ["rm %s" % obj]
        LOG.debug('Running %s', ' '.join(cmd))
        self.execute_command(cmd)

    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :param path: Path to file
        :type path: str
        :return: Standard output.
        """
        cmd = ["cat %s" % path]
        return self._get_remote_stdout(cmd)

    def _write_status(self, status):
        """
        Write status

        :param status: Status fo write
        :type status: str
        """
        raw_status = base64.b64encode(json.dumps(status))
        cmd = [
            "echo {raw_status} > "
            "{status_file}".format(raw_status=raw_status,
                                   status_file=self.status_path)
        ]
        self.execute_command(cmd)

    def _read_status(self):
        """
        Read status

        :return: Status in JSON format, if it exist
        """
        if self._status_exists():
            cmd = ["cat %s" % self.status_path]
            _, stdout, _ = self.execute_command(cmd)
            return json.loads(base64.b64decode(stdout.read()))
        else:
            return self._empty_status

    def _status_exists(self):
        """
        Check, if status exist

        :return: Exist status
        :rtype: bool
        :raise SshDestinationError: if any error.
        """
        cmd = ["bash -c 'if test -s %s; "
               "then echo exists; "
               "else echo not_exists; "
               "fi'" % self.status_path]
        LOG.debug('Running %r', cmd)
        _, stdout, _ = self.execute_command(cmd)
        output = stdout.read()
        if output.strip() == 'exists':
            return True
        elif output.strip() == 'not_exists':
            return False
        else:
            raise SshDestinationError('Unrecognized response: %s' % output)

    def share(self, url):
        super(Ssh, self).share(url)

    def execute_command(self, cmd):
        """Execute ssh command

        :param cmd: Command for execution
        :type cmd: list
        :return: Handlers of stdin, stdout and stderr
        :rtype: tuple
        :raise SshDestinationError: if any error
        """
        cmd_str = ' '.join(cmd)
        try:
            with self._shell() as shell:
                stdin_, stdout_, stderr_ = shell.exec_command(cmd_str)
                exit_code = stdout_.channel.recv_exit_status()
                if exit_code != 0:
                    LOG.error("Failed while execute command %s:  %s",
                              cmd_str, stderr_.read())
                    raise SSHException('%s exited with code %d'
                                       % (cmd_str, exit_code))
                return stdin_, stdout_, stderr_

        except SSHException as err:
            LOG.error('Failed to execute %s', cmd_str)
            raise SshDestinationError(err)

    @contextmanager
    def _get_remote_stdout(self, cmd):
        """Get remote stdout handler

        :param cmd: Command for execution
        :type cmd: list
        :return: Remote stdout handler
        :rtype: generator
        :raise SshDestinationError: if any error
        """
        cmd_str = ' '.join(cmd)
        try:
            with self._shell() as shell:
                _, stdout, _ = shell.exec_command(cmd_str)
                yield stdout

        except SSHException as err:
            LOG.error('Failed to execute %s', cmd_str)
            raise SshDestinationError(err)

    def _mkdirname_r(self, remote_name):
        """Create directory for a given file on the destination.
        For example, for a given file '/foo/bar/xyz' it would create
        directory '/foo/bar/'.

        :param remote_name: Full path to a file
        :type remote_name: str
        """
        return self._mkdir_r(os.path.dirname(remote_name))

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
            shell.connect(hostname=self.ssh_connect_info.host,
                          key_filename=self.ssh_connect_info.key,
                          port=self.ssh_connect_info.port,
                          username=self.ssh_connect_info.user)
            yield shell
        except (AuthenticationException, SSHException, socket.error) as err:
            raise SshDestinationError(err)
        finally:
            shell.close()

    def netcat(self, command):
        """
        Run netcat on the destination pipe it to a given command.

        :return: Port number to where netcat listens to.
        :rtype: int
        :raise SshDestinationError: if failed to start netcat.
        """
        max_port_number = 65000
        port = 9990

        while port < max_port_number:
            try:
                self.execute_command(['nc -l %d | %s'
                                      % (port, command)])
                return
            except SshDestinationError as err:
                LOG.warning(err)
                port += 1

        raise SshDestinationError('Failed to start netcat '
                                  'on the remote server')
