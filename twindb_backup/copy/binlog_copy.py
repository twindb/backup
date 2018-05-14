"""Class to describe Binlog backup copy"""

from twindb_backup.copy.base_copy import BaseCopy


class BinlogCopy(BaseCopy):  # pylint: disable=too-few-public-methods
    """
    Instantiate a Binlog copy.

    :param host: Hostname where the backup was taken from.
    :type host: str
    :param name: Base name of the backup copy file as it's stored
        on the destination.
    :type name: str
    :param time_created: Time when copy created
    :type time_created: int
    :raise WrongInputData: if type is neither full or incremental,
        if name is not a basename.
    """
    def __init__(self, host, name, time_created):
        super(BinlogCopy, self).__init__(host, name)
        self._time_created = time_created
        self._source_type = 'binlog'

    @property
    def _extra_path(self):
        return None
