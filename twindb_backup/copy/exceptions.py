"""Module for Backup copy exception classes"""
from twindb_backup.exceptions import TwinDBBackupError


class BackupCopyError(TwinDBBackupError):
    """General backup copy error"""


class UnknownSourceType(BackupCopyError):
    """Raises when source type is not set"""


class WrongInputData(BackupCopyError):
    """Raises when incorrent inputs are used"""
