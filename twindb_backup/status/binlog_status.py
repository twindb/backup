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
        # TODO: Need to implement (blocked by PR)
        pass

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
