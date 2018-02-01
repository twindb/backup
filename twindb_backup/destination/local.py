# -*- coding: utf-8 -*-
"""
Module defines Local destination.
"""
import os
import socket
from subprocess import Popen

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import StatusFileError
from twindb_backup.util import run_command


class Local(BaseDestination):
    """
    Local destination class.
    """
    def __init__(self, path=None):
        super(Local, self).__init__()
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
        self.status_tmp_path = self.status_path + ".tmp"

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

    def list_files(self, prefix, recursive=False):

        if recursive:
            ls_cmd = ["ls", "-R", "%s*" % prefix]
        else:
            ls_cmd = ["ls", "%s*" % prefix]

        cmd = ls_cmd

        with run_command(cmd) as cout:
            return sorted(str(cout).split())

    def find_files(self, prefix, run_type):

        cmd = ["find", "%s*" % prefix, "-type", "f"]

        with run_command(cmd) as cout:
            return sorted(str(cout).split())

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

    def _write_status(self, status):
        with open(self.status_tmp_path, 'w') as fstatus:
            fstatus.write(status)
        if not self._is_valid_status(self.status_tmp_path):
            raise StatusFileError("Valid status file not found")
        self._move_file(self.status_tmp_path, self.status_path)

    def _get_file_content(self, path):
        with open(path) as f_descr:
            return f_descr.read()

    def _move_file(self, source, destination):
        cmd = ["cp", "-rf", source, destination]
        run_command(cmd)

    def _status_exists(self):
        return os.path.exists(self.status_path)
