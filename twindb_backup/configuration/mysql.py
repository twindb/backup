"""MySQL instance configuration"""
from twindb_backup import INTERVALS, XTRABACKUP_BINARY, XBSTREAM_BINARY


class MySQLConfig(object):
    """
    MySQL Instance configuration
    """
    def __init__(self, **kwargs):

        self._defaults_file = kwargs.get(
            'mysql_defaults_file',
            '/root/.my.cnf'
        )
        self._full_backup = kwargs.get('full_backup', INTERVALS[1])
        self._expire_log_days = int(kwargs.get('expire_log_days', 7))
        self._xtrabackup_binary = kwargs.get(
            'xtrabackup_binary',
            XTRABACKUP_BINARY
        )
        self._xbstream_binary = kwargs.get('xbstream_binary', XBSTREAM_BINARY)

    @property
    def defaults_file(self):
        """Path to .my.cnf with MySQL credentials."""

        return self._defaults_file

    @property
    def full_backup(self):
        """How often to take full backups e.g. ``daily``."""

        return self._full_backup

    @property
    def expire_log_days(self):
        """For how many days keep binlog copies"""

        return self._expire_log_days

    @property
    def xtrabackup_binary(self):
        """Path to xtrabackup binary"""

        return self._xtrabackup_binary

    @xtrabackup_binary.setter
    def xtrabackup_binary(self, path):
        """Set path to Xtrabackup"""
        self._xtrabackup_binary = path

    @property
    def xbstream_binary(self):
        """Path to xbstream binary"""

        return self._xbstream_binary

    @xbstream_binary.setter
    def xbstream_binary(self, path):
        """Set path to xbstream"""

        self._xbstream_binary = path
