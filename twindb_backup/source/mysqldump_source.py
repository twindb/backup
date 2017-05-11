# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up local MySQL with mysqldump.
"""
from contextlib import contextmanager

from twindb_backup.source.base_source import BaseSource


class MysqldumpSourceError(Exception):
    """Errors during backups"""


class MysqldumpSource(BaseSource):
    """Mysqldump class"""
    def __init__(self, run_type, defaults_file):
        """MysqldumpSource constructor

        :param run_type: 'daily', 'weekly', etc
        :type run_type: str
        """
        self.defaults_file = defaults_file
        super(MysqldumpSource, self).__init__(run_type)

    @contextmanager
    def get_stream(self):
        """Run mysqldump and return pipe with backup content"""
        pass
