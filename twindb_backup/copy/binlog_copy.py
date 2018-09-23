"""Class to describe Binlog backup copy"""

from twindb_backup.copy.base_copy import BaseCopy


class BinlogCopy(BaseCopy):  # pylint: disable=too-few-public-methods
    """
    Instantiate a Binlog copy in status

    :param host: Hostname where the backup was taken from.
    :type host: str
    :param name: Base name of the backup copy file as it's stored
        on the destination.
    :type name: str
    :param created_at: Time when copy created
    :type created_at: int
    """
    def __init__(self, host, name, created_at):
        super(BinlogCopy, self).__init__(host, name)
        self._created_at = int(created_at)
        self._source_type = 'binlog'

    def __eq__(self, other):
        """
        Compare two instances.

        :param other:
        :type other: BinlogCopy
        :return:
        """
        return all(
            (
                self.created_at == other.created_at,
                self.key == other.key,
            )
        )

    def __str__(self):
        return "%s: file name: %s, created at: %d" % (
            self.__class__.__name__,
            self.name,
            self.created_at
        )

    @property
    def _extra_path(self):
        return None

    @property
    def created_at(self):
        """Time of created copy"""
        return self._created_at

    @property
    def name(self):
        """Binlog copy name as in SHOW BINARY LOGS."""
        return self._name
