"""Base class for a backup copy"""
from twindb_backup import INTERVALS
from twindb_backup.copy.exceptions import UnknownSourceType, WrongInputData


class BaseCopy(object):
    """Base class for a backup copy in status."""
    def __init__(self, host, run_type, name):
        self._host = host
        if run_type in INTERVALS:
            self._run_type = run_type
        else:
            raise WrongInputData(
                'Wrong value of run_type: %s. Must be one of %s'
                % (run_type, ", ".join(INTERVALS))
            )
        self._name = name
        self._source_type = None

    @property
    def run_type(self):
        """What run type was when the backup copy was taken."""
        return self._run_type

    @property
    def key(self):
        """It's a relative path backup copy.
        It's relative to the remote path as from the twindb config.
        It's also a key in status, hence the name."""
        if self._source_type:
            return "{host}/{run_type}/{source_type}/{name}".format(
                host=self._host,
                run_type=self._run_type,
                name=self._name,
                source_type=self._source_type
            )
        else:
            raise UnknownSourceType("Source type is not defined")

    def __getitem__(self, item):
        return getattr(self, item)
