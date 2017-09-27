# -*- coding: utf-8 -*-
"""
Module for SSH destination.
"""
import base64
import json
import os
import socket
from subprocess import Popen, PIPE

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError
from twindb_backup.util import run_command


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

        self.user = ssh_connect_info.user
        self.key = ssh_connect_info.key
        self.port = ssh_connect_info.port
        self.host = ssh_connect_info.host

        self._ssh_command = ['ssh', '-l', self.user,
                             '-o',
                             'StrictHostKeyChecking=no',
                             '-o',
                             'PasswordAuthentication=no',
                             '-p', str(self.port),
                             '-i', self.key,
                             self.host]
        self.status_path = "{remote_path}/{hostname}/status".format(
            remote_path=self.remote_path,
            hostname=hostname
        )

    def save(self, handler, name):
        """
        Read from handler and save it on remote ssh server

        :param name: store backup copy as this name
        :param handler:
        :return: exit code
        """
        remote_name = self.remote_path + '/' + name
        self._mkdir_r(os.path.dirname(remote_name))
        cmd = self._ssh_command + ["cat - > \"%s\"" % remote_name]
        return self._save(cmd, handler)

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :return: exit code
        """
        cmd = self._ssh_command + ["mkdir -p \"%s\"" % path]
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        _, cerr = proc.communicate()

        if proc.returncode:
            LOG.error('Failed to create directory %s: %s', path, cerr)
            exit(1)

        return proc.returncode

    def list_files(self, prefix, recursive=False):

        if recursive:
            ls_cmd = ["ls -R %s*" % prefix]
        else:
            ls_cmd = ["ls %s*" % prefix]

        cmd = self._ssh_command + ls_cmd

        with run_command(cmd, ok_non_zero=True) as cout:
            return sorted(cout.read().split())

    def find_files(self, prefix, run_type):

        cmd = self._ssh_command + [
            "find {prefix}/*/{run_type} "
            "-type f".format(prefix=prefix,
                             run_type=run_type)
        ]

        with run_command(cmd, ok_non_zero=True) as cout:
            return sorted(cout.read().split())

    def delete(self, obj):
        cmd = self._ssh_command + ["rm %s" % obj]
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd)
        proc.communicate()

    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :return: Standard output.
        """
        cmd = self._ssh_command + ["cat %s" % path]
        return run_command(cmd)

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))
        cmd = self._ssh_command + [
            "echo {raw_status} > "
            "{status_file}".format(raw_status=raw_status,
                                   status_file=self.status_path)
        ]
        proc = Popen(cmd)
        _, cerr = proc.communicate()
        if proc.returncode:
            LOG.error('Failed to write backup status')
            LOG.error(cerr)
            exit(1)
        return status

    def _read_status(self):

        if self._status_exists():
            cmd = self._ssh_command + ["cat %s" % self.status_path]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            cout, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('Failed to read backup status: %d: %s',
                          proc.returncode,
                          cerr)
                exit(1)
            return json.loads(base64.b64decode(cout))
        else:
            return self._empty_status

    def _status_exists(self):
        cmd = self._ssh_command + \
              ["bash -c 'if test -s %s; "
               "then echo exists; "
               "else echo not_exists; "
               "fi'" % self.status_path]

        try:
            LOG.debug('Running %r', cmd)
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            cout, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('Failed to read backup status: %d: %s',
                          proc.returncode,
                          cerr)
                exit(1)
            if cout.strip() == 'exists':
                return True
            elif cout.strip() == 'not_exists':
                return False
            else:
                raise DestinationError('Unrecognized response: %s' % cout)

        except OSError as err:
            LOG.error('Failed to run %s: %s', " ".join(cmd), err)
            exit(1)

    def share(self, url):
        super(Ssh, self).share(url)
