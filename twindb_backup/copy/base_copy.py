"""Base class for a backup copy"""

from abc import abstractproperty

from twindb_backup.copy.exceptions import UnknownSourceType


class BaseCopy(object):  # pylint: disable=too-few-public-methods
    """Base class for a backup copy in status."""
    def __init__(self, host, name):
        self._host = host
        self._name = name
        self._source_type = None

    @property
    def key(self):
        """It's a relative path backup copy.
        It's relative to the remote path as from the twindb config.
        It's also a key in status, hence the name."""
        if self._source_type:
            if self._extra_path is not None:
                return "{host}/{extra_path}/{source_type}/{name}".format(
                    host=self._host,
                    extra_path=self._extra_path,
                    name=self._name,
                    source_type=self._source_type
                )
            return "{host}/{source_type}/{name}".format(
                host=self._host,
                name=self._name,
                source_type=self._source_type
            )
        else:
            raise UnknownSourceType("Source type is not defined")

    @abstractproperty
    def _extra_path(self):
        """Property that describes additional path to key"""
