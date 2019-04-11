"""
This module describes class to work with backup copies of the file type.
"""
from twindb_backup.copy.periodic_copy import PeriodicCopy


class FileCopy(PeriodicCopy):
    """
    Backup copy of a file or directory.

    :param host: hostname where the backup was taken from.
    :type host: str
    :param name: backup copy basename
    :type name: str
    :param run_type: run type. daily, hourly, etc.
    :type run_type: str
    """
    def __init__(self, *args, **kwargs):
        super(FileCopy, self).__init__(*args, **kwargs)
        self._source_type = 'files'
