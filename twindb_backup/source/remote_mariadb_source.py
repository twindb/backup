"""
Module defines MySQL source class for backing up local MariaDB server.
"""

from twindb_backup import MARIABACKUP_BINARY
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


class RemoteMariaDBSource(RemoteMySQLSource):
    def get_stream(self):
        raise NotImplementedError("Method get_stream not implemented")

    def __init__(self, kwargs):
        super().__init__(kwargs)
        self._xtrabackup = kwargs.get("xtrabackup_binary") or MARIABACKUP_BINARY
