"""Status exceptions."""
from twindb_backup.exceptions import TwinDBBackupError


class StatusError(TwinDBBackupError):
    """General status error"""


class CorruptedStatus(StatusError):
    """Status file is corrupt"""


class StatusKeyNotFound(StatusError):
    """Accessing a key that doesn't exist"""
