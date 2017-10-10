# -*- coding: utf-8 -*-
"""
Module for SSH destination.
"""
import base64
import json
import socket

import os
from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.ssh.client import SshClient


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
        if remote_path:
            self.remote_path = remote_path.rstrip('/')

        self._ssh_client = SshClient(ssh_connect_info)

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
        cmd = 'cat - > ' + remote_name
        self.execute_command(cmd)

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :type path: str
        """
        cmd = 'mkdir -p "%s"' % path
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
        ls_options = ""

        if recursive:
            ls_options = "-R"

        ls_cmd = "ls {ls_options} {prefix}".format(
            ls_options=ls_options,
            prefix=prefix
        )

        with self._ssh_client.get_remote_handlers(ls_cmd) as (_, cout, _):
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
        cmd = "find {prefix}/*/{run_type} -type f".format(
            prefix=prefix,
            run_type=run_type
        )

        with self._ssh_client.get_remote_handlers(cmd) as (_, cout, _):
            return sorted(cout.read().split())

    def delete(self, obj):
        """
        Delete file by path

        :param obj: path to a remote file.
        """
        cmd = "rm %s" % obj
        self.execute_command(cmd)

    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :param path: Path to file
        :type path: str
        :return: Standard output.
        """
        cmd = "cat %s" % path
        return self._ssh_client.get_remote_handlers(cmd)[1]

    def _write_status(self, status):
        """
        Write status

        :param status: Status fo write
        :type status: str
        """
        raw_status = base64.b64encode(json.dumps(status))
        cmd = "echo {raw_status} > {status_file}".format(
            raw_status=raw_status,
            status_file=self.status_path
        )
        self.execute_command(cmd)

    def _read_status(self):
        """
        Read status

        :return: Status in JSON format, if it exist
        """
        if self._status_exists():
            cmd = "cat %s" % self.status_path
            with self._ssh_client.get_remote_handlers(cmd) as (_, stdout, _):
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
        cmd = "bash -c 'if test -s %s; " \
              "then echo exists; " \
              "else echo not_exists; " \
              "fi'" % self.status_path
        with self._ssh_client.get_remote_handlers(cmd) as (_, cout, _):
            if cout.read().strip() == 'exists':
                return True
            elif cout.read().strip() == 'not_exists':
                return False
            else:
                raise SshDestinationError('Unrecognized response: %s'
                                          % cout.read())

    def execute_command(self, cmd):
        """Execute ssh command

        :param cmd: Command for execution
        :type cmd: str
        :return: Handlers of stdin, stdout and stderr
        :rtype: tuple
        """
        LOG.debug('Executing: %s', cmd)
        return self._ssh_client.execute(cmd)

    @property
    def client(self):
        """Return client"""
        return self._ssh_client

    def _mkdirname_r(self, remote_name):
        """Create directory for a given file on the destination.
        For example, for a given file '/foo/bar/xyz' it would create
        directory '/foo/bar/'.

        :param remote_name: Full path to a file
        :type remote_name: str
        """
        return self._mkdir_r(os.path.dirname(remote_name))

    def netcat(self, command, port=9990):
        """
        Run netcat on the destination pipe it to a given command.

        """
        try:
            return self.execute_command('nc -l %d | %s' % (port, command))
        except SshDestinationError as err:
            LOG.error(err)
