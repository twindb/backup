"""
Module defines MySQL source class for backing up local MariaDB server.
"""

from twindb_backup import MARIABACKUP_BINARY
from twindb_backup.source.mysql_source import MySQLSource


class MariaDBSource(MySQLSource):
    def __init__(self, mysql_connect_info, run_type, backup_type, **kwargs):
        super().__init__(mysql_connect_info, run_type, backup_type, **kwargs)
        self._xtrabackup = kwargs.get("xtrabackup_binary") or MARIABACKUP_BINARY
