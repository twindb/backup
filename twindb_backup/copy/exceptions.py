"""Module for Backup copy exception classes"""


class BackupCopyError(Exception):
    """General backup copy error"""


class UnknownSourceType(BackupCopyError):
    """Raises when source type is not set"""


class WrongInputData(BackupCopyError):
    """Raises when incorrent inputs are used"""
