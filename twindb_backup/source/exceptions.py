"""
Module for backup source exceptions.
"""

from twindb_backup.exceptions import TwinDBBackupError


class SourceError(TwinDBBackupError):
    """General source error"""

    def __str__(self):
        return "%s: %s" % (self.__class__, self.message)


class MySQLSourceError(SourceError):
    """Exceptions in MySQL source"""


class RemoteMySQLSourceError(MySQLSourceError):
    """Exceptions in remote MySQL source"""


class BinlogSourceError(SourceError):
    """Exceptions in Binlog source"""
