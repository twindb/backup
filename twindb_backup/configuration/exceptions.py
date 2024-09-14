"""TwinDB Backup configuration exceptions."""

from twindb_backup.exceptions import TwinDBBackupError


class ConfigurationError(TwinDBBackupError):
    """Base configuration error"""
