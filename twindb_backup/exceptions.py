"""Module that describes exceptions of twindb_backup module."""


class TwinDBBackupError(Exception):
    """Catch-all exceptions"""


class OperationError(TwinDBBackupError):
    """Exceptions that prevent normal TwinDB Backup operation"""


class LockWaitTimeoutError(TwinDBBackupError):
    """Timeout expired while waiting for a lock"""


class TwinDBBackupInternalError(TwinDBBackupError):
    """Internal errors in the tool itself"""
