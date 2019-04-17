"""Binlog status is a class for a binlog copies status.
"""
import json
from base64 import b64encode

from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.base_status import BaseStatus


class BinlogStatus(BaseStatus):
    """Binlog class for status"""

    def __init__(self, content=None, dst=None, status_directory=None):
        super(BinlogStatus, self).__init__(
            content=content,
            dst=dst,
            status_directory=status_directory
        )

    @property
    def basename(self):
        return 'binlog-status'

    def _status_serialize(self):
        return b64encode(
            json.dumps(
                self._as_dict()
            )
        )

    def _load(self, status_as_json):

        self._status = []
        for key, value in json.loads(status_as_json).iteritems():
            host = key.split('/')[0]
            name = key.split('/')[2]
            try:
                created_at = value['created_at']
            except KeyError:
                created_at = value['time_created']
            copy = BinlogCopy(
                host=host,
                name=name,
                created_at=created_at
            )
            self._status.append(copy)

        return self._status

    def _as_dict(self):
        status = {}
        for copy in self:
            status[copy.key] = {
                'time_created': copy.created_at
            }
        return status

    def __eq__(self, other):
        for copy in self:
            if copy not in other:
                return False
        for copy in other:
            if copy not in self:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)
