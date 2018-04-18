"""Base status is a class for a general purpose status.
For now status is created/maintained for MySQL copies only.
"""
import json
from base64 import b64encode

from twindb_backup import INTERVALS
from twindb_backup.status.exceptions import StatusError, StatusKeyNotFound


class BaseStatus(object):
    """Base class for status."""
    __version__ = None
    _hourly = {}
    _daily = {}
    _weekly = {}
    _monthly = {}
    _yearly = {}

    def __init__(self):
        for i in INTERVALS:
            setattr(self, '_%s' % i, {})

    def __eq__(self, other):
        return all(
            (
                self._hourly == other.hourly,
                self._daily == other.daily,
                self._weekly == other.weekly,
                self._monthly == other.monthly,
                self._yearly == other.yearly,
            )
        )

    def __str__(self):
        status = {}
        for i in INTERVALS:
            status[i] = {}
            period_copies = getattr(self, i)
            for key, value in period_copies.iteritems():
                status[i][key] = value.as_dict()
        return json.dumps(status)

    @property
    def valid(self):
        """
        Returns True if status is valid.
        """
        return all(
            (
                self._hourly is not None,
                self._daily is not None,
                self._weekly is not None,
                self._monthly is not None,
                self._yearly is not None
            )
        )

    @property
    def version(self):
        """
        Version of status file. Originally status file didn't have
        any versions, but in future the version will be used to work
        with new features.
        """
        return self.__version__

    @property
    def hourly(self):
        """Dictionary with hourly backups"""
        return self._hourly

    @property
    def daily(self):
        """Dictionary with daily backups"""
        return self._daily

    @property
    def weekly(self):
        """Dictionary with weekly backups"""
        return self._weekly

    @property
    def monthly(self):
        """Dictionary with monthly backups"""
        return self._monthly

    @property
    def yearly(self):
        """Dictionary with yearly backups"""
        return self._yearly

    def add(self, backup_copy):
        """
        Add entry to status.

        :param backup_copy: Instance of backup copy
        :type backup_copy: BaseCopy
        :return: Nothing
        """
        getattr(self, backup_copy.run_type)[backup_copy.key] = backup_copy

    def remove(self, period, key):
        """
        Remove key from the status.

        :param period: one of 'hourly', 'daily', 'weekly', 'monthly', 'yearly'
        :type period: str
        :param key: Backup name in status. It's a relative file name
            of a backup copy. For example,
            master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz
        :type key: str
        :raise StatusKeyNotFound: if there is no such key in the status
        """
        if period not in INTERVALS:
            raise StatusError('Wrong period %s. Valid values are %s'
                              % (period, ", ".join(INTERVALS)))
        try:
            del getattr(self, "_%s" % period)[key]
        except KeyError:
            raise StatusKeyNotFound(
                "There is no %s in %s backups"
                % (key, period)
            )

    def serialize(self):
        """
        Return a string that represents current state
        """

        return b64encode(self.__str__())

    def backup_duration(self, run_type, key):
        """
        By given backup identifier (run_type, key) return its duration.

        :param run_type:
        :param key:
        :return: backup duration in seconds
        :rtype: int
        """
        return getattr(self, run_type)[key].duration

    def get_latest_backup(self):
        """
        Find the latest backup copy.

        :return: backup copy
        :rtype: BaseCopy
        """
        latest_copy = None
        latest_backup_time = 0
        for i in INTERVALS:
            period_copies = getattr(self, i)
            for key, value in period_copies.iteritems():
                if value.backup_started > latest_backup_time:
                    latest_copy = key
                    latest_backup_time = value.backup_started

        return latest_copy
