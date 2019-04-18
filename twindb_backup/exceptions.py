"""Module that describes exceptions of twindb_backup module"""


class TwinDBBackupError(Exception):
    """Catch-all exceptions"""


class OperationError(TwinDBBackupError):
    """High level exceptions of twindb_backup package"""


class LockWaitTimeoutError(TwinDBBackupError):
    """Class that describes exception of lock wait timeout"""


class TwinDBBackupInternalError(TwinDBBackupError):
    """Internal errors in the tool itself"""
