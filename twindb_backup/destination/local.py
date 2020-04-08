# -*- coding: utf-8 -*-
"""
Module defines Local destination.
"""
from subprocess import Popen

from os import path as osp

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.util import run_command, mkdir_p


class Local(BaseDestination):
    """
    Local destination class.
    """
    def __init__(self, path=None):
        super(Local, self).__init__(path)
        self._path = path
        self.remote_path = self.path
        mkdir_p(self.path)

    def get_stream(self, copy):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :param copy: Backup copy
        :type copy: BaseCopy
        :return:
        """
        path = "%s/%s" % (self.remote_path, copy.key)
        cmd = ["cat", path]
        return run_command(cmd)

    @property
    def path(self):
        """
        Root path on local file system where local backup copies are stored.
        """
        mkdir_p(self._path)
        return self._path

    def read(self, filepath):
        with open(osp.join(self.path, filepath), 'r') as fdesc:
            return fdesc.read()

    def save(self, handler, filepath):
        """
        Read from handler and save it on local storage

        :param filepath: store backup copy as this name
        :param handler: Input stream
        """
        with handler as in_stream:
            proc = Popen(
                ["cat", "-"],
                stdin=in_stream,
                stdout=open(
                    osp.join(
                        self.path,
                        filepath
                    ),
                    'wb'
                )
            )
            proc.wait()

    def _list_files(self, prefix=None, recursive=False, files_only=False):
        rec_cond = "" if recursive else " -maxdepth 1"
        fil_cond = " -type f" if files_only else ""

        cmd_str = "bash -c 'if test -d {prefix} ; " \
                  "then find {prefix}{recursive}{files_only}; fi'"
        cmd_str = cmd_str.format(
            prefix=prefix,
            recursive=rec_cond,
            files_only=fil_cond
        )
        cmd = [
            'bash', '-c', cmd_str
        ]
        with run_command(cmd) as cout:

            if files_only:
                return cout.read().decode("utf-8").split()
            else:
                return cout.read().decode("utf-8").split()[1:]

    def delete(self, path):
        cmd = ["rm", path]
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd)
        proc.communicate()

    def write(self, content, filepath):
        with open(osp.join(self.path, filepath), 'w') as fdesc:
            fdesc.write(content)

    def __str__(self):
        return "Local(%s)" % self.path
