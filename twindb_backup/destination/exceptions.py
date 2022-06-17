"""
Module for destination exceptions.
"""
from twindb_backup.exceptions import TwinDBBackupError


class DestinationError(TwinDBBackupError):
    """General destination error"""

    pass


class FileNotFound(DestinationError):
    """File doesn't exist on destination"""

    pass


class S3DestinationError(DestinationError):
    """S3 destination errors"""

    pass


class GCSDestinationError(DestinationError):
    """GCS destination errors"""

    pass


class SshDestinationError(DestinationError):
    """SSH destination errors"""

    pass


class AzureBlobDestinationError(DestinationError):
    """Azure-blob destination errors"""
    pass
