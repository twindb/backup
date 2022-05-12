"""
Module for exporters exceptions.
"""
from twindb_backup.exceptions import TwinDBBackupError


class BaseExporterError(TwinDBBackupError):
    """General exporters error"""

    pass


class DataDogExporterError(BaseExporterError):
    """DataDog exporters error"""

    pass

class StatsdExporterError(BaseExporterError):
    """Statsd exporters error"""

    pass
