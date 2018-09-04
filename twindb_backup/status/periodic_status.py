"""Periodic status is a class for a periodic copies status.
For now status is created/maintained for MySQL copies only.
"""
from twindb_backup import INTERVALS
from twindb_backup.status.base_status import BaseStatus


class PeriodicStatus(BaseStatus):
    """Periodic class for status"""

    def _status_serialize(self):
        raise NotImplementedError

    @property
    def basename(self):
        raise NotImplementedError

    def _load(self, status_as_json):
        raise NotImplementedError

    def __init__(self, content=None):
        super(PeriodicStatus, self).__init__(content=content)

    def __eq__(self, other):
        comparison = ()
        for interval in INTERVALS:
            comparison += (
                getattr(self, interval) == getattr(other, interval),
            )

        return all(comparison)

    def __run_type(self, run_type):
        result = {}
        for copy in self._status:
            if copy.run_type == run_type:
                result[copy.key] = copy
        return result

    def _status_serialize(self):
        raise NotImplementedError

    def _load(self, status_as_json):
        raise NotImplementedError

    @property
    def hourly(self):
        """Dictionary with hourly backups"""
        return self.__run_type('hourly')

    @property
    def daily(self):
        """Dictionary with daily backups"""
        return self.__run_type('daily')

    @property
    def weekly(self):
        """Dictionary with weekly backups"""
        return self.__run_type('weekly')

    @property
    def monthly(self):
        """Dictionary with monthly backups"""
        return self.__run_type('monthly')

    @property
    def yearly(self):
        """Dictionary with yearly backups"""
        return self.__run_type('yearly')
