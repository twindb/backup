"""Binlog status is a class for a binlog copies status.
"""
import json

from twindb_backup.status.base_status import BaseStatus
from twindb_backup.status.exceptions import StatusKeyNotFound


class BinlogStatus(BaseStatus):
    """Binlog class for status"""

    copies = {}

    def add(self, backup_copy):
        self.copies[backup_copy.key] = backup_copy

    def remove(self, key, period=None):
        try:
            del self.copies[key]
        except KeyError:
            raise StatusKeyNotFound(
                "There is no %s in backups" % key
            )

    def get_latest_backup(self):
        latest_copy = None
        latest_created_time = 0
        for key, value in self.copies.iteritems():
            if value.time_created > latest_created_time:
                latest_copy = key
                latest_created_time = value.time_created
        return latest_copy

    def __getitem__(self, item):
        if item in self.copies:
            return self.copies[item]
        return None

    def __str__(self):
        return json.dumps(self.copies,
                          indent=4,
                          sort_keys=True)

    @property
    def valid(self):
        return self.copies is not None
