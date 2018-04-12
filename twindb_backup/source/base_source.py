# -*- coding: utf-8 -*-
"""
Module defines base source class.
"""
import ConfigParser
import socket

import time

from twindb_backup import delete_local_files, INTERVALS, LOG


class BaseSource(object):
    """
    Base source for backup
    """
    run_type = None
    _suffix = None
    _media_type = None
    _intervals = INTERVALS
    _name = None
    _host = None
    _created_at = None
    _file_name_prefix = ''

    def __init__(self, run_type):
        """
        Construct instance of BaseSource()

        :param run_type: Run type e.g. hourly, daily, etc
        :type run_type: str
        """
        self.run_type = run_type
        self._host = socket.gethostname()
        self._created_at = time.strftime('%Y-%m-%d_%H_%M_%S')

    def get_stream(self):
        """
        Get backup stream in a handler
        """

    def get_prefix(self):
        """
        Get prefix of the backup copy. It includes hostname and run type.

        :return: Backup name prefix like 'db-10/daily'
        """
        return "{hostname}/{run_type}".format(
            run_type=self.run_type,
            hostname=self._host
        )

    def _get_name(self, filename_prefix):

        LOG.debug('Suffix = %s', self.suffix)
        self._name = "{prefix}/{media_type}/{file}-{time}.{suffix}".format(
            prefix=self.get_prefix(),
            media_type=self._media_type,
            file=filename_prefix,
            time=self._created_at,
            suffix=self._suffix
        )
        return self._name

    def _delete_local_files(self, filename, config):
        try:
            keep_copies = config.getint('retention_local',
                                        '%s_copies' % self.run_type)
            keep_local_path = config.get('destination', 'keep_local_path')
            dir_backups = "{local_path}/{prefix}/{media_type}/{file}*".format(
                local_path=keep_local_path,
                prefix=self.get_prefix(),
                media_type=self._media_type,
                file=filename
            )
            delete_local_files(dir_backups, keep_copies)

        except ConfigParser.NoOptionError:
            pass

    @property
    def suffix(self):
        """Backup file name suffix"""
        return self._suffix

    @suffix.setter
    def suffix(self, suffix):
        self._suffix = suffix

    @property
    def basename(self):
        """Return file name (w/o directory part) of the backup."""
        return "{filename_prefix}-{time}.{suffix}".format(
            filename_prefix=self._file_name_prefix,
            time=self._created_at,
            suffix=self._suffix
        )

    @property
    def host(self):
        """Return host where the backup is being taken from."""
        return self._host
