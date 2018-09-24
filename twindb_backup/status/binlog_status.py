"""Binlog status is a class for a binlog copies status.
"""
import json
from base64 import b64encode

from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.base_status import BaseStatus
from twindb_backup.status.exceptions import StatusKeyNotFound


class BinlogStatus(BaseStatus):
    """Binlog class for status"""

    @property
    def basename(self):
        return 'binlog-status'

    def __init__(self, content=None):
        super(BinlogStatus, self).__init__(content=content)

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

    def add(self, backup_copy):
        self._status.append(backup_copy)

    def remove(self, key):
        """
        Remove key from the status.

        :param key: Backup name in status. It's a relative file name
            of a backup copy. For example,
            master1/binlog/mysqlbinlog0001.bin
        :type key: str
        :raise StatusKeyNotFound: if there is no such key in the status
        """
        for copy in self._status:
            if copy.key == key:
                self._status.remove(copy)
                return
        raise StatusKeyNotFound("There is no %s in backups" % key)

    def _as_dict(self):
        status = {}
        for copy in self:
            status[copy.key] = {
                'time_created': copy.created_at
            }
        return status
