# -*- coding: utf-8 -*-
"""
Module defines Local destination.
"""
import os
import socket
from subprocess import Popen

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import DestinationError
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

    def get_files(self, prefix, copy_type=None, interval=None):
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

    def write_file(self, content, path):
        with open(path, 'w') as fstatus:
            fstatus.write(content)

    def read_file(self, path):
        if self.is_file_exist(path):
            with open(path) as status_descriptor:
                cout = status_descriptor.read()
                return cout
        else:
            raise DestinationError("File not found")

    def is_file_exist(self, path):
        return os.path.exists(path)
