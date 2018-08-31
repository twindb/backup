# -*- coding: utf-8 -*-
"""
Module defines Local destination.
"""
import os
import socket
from subprocess import Popen

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.status.mysql_status import MySQLStatus
from twindb_backup.util import run_command


class Local(BaseDestination):
    """
    Local destination class.
    """
    def __init__(self, path=None):
        super(Local, self).__init__(path)
        self.path = path
        self.remote_path = self.path
        try:
            os.mkdir(self.path)
        except OSError as err:
            if err.errno == 17:  # OSError: [Errno 17] File exists
                pass
            else:
                raise
        self.status_path = "{path}/{hostname}/status".format(
            path=self.path,
            hostname=socket.gethostname()
        )

    def __str__(self):
        return "Local(%s)" % self.path

    def save(self, handler, name):
        """
        Read from handler and save it on local storage

        :param name: store backup copy as this name
        :param handler:
        :return: exit code
        """
        local_name = self.path + '/' + name
        cmd = ["cat", "-", local_name]
        self._save(cmd, handler)

    def _list_files(self, path, recursive=False, files_only=False):
        rec_cond = "" if recursive else " -maxdepth 1"
        fil_cond = " -type f" if files_only else ""

        cmd_str = "bash -c 'if test -d {path} ; " \
                  "then find {path}{recursive}{files_only}; fi'"
        cmd_str = cmd_str.format(
            path=path,
            recursive=rec_cond,
            files_only=fil_cond
        )
        cmd = [
            'bash', '-c', cmd_str
        ]
        with run_command(cmd) as cout:

            if files_only:
                return cout.read().split()
            else:
                return cout.read().split()[1:]

    def delete(self, obj):
        cmd = ["rm", obj]
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd)
        proc.communicate()

    @staticmethod
    def get_stream(path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :return:
        """
        cmd = ["cat", path]
        return run_command(cmd)

    def _read_status(self):
        if not self._status_exists():
            return MySQLStatus()

        with open(self.status_path) as status_descriptor:
            cout = status_descriptor.read()
            return MySQLStatus(content=cout)

    def _write_status(self, status):
        with open(self.status_path, 'w') as fstatus:
            fstatus.write(status.serialize())

    def _status_exists(self):
        return os.path.exists(self.status_path)
