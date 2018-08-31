"""Interval class for a backup copy"""
from twindb_backup import INTERVALS
from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.copy.exceptions import WrongInputData


class PeriodicCopy(BaseCopy):
    """Interval class for a periodic backup copy in status

    :param host: Hostname where the backup was taken from.
    :type host: str
    :param run_type: Run type when the backup was taken: daily, weekly, etc.
    :type run_type: str
    :param name: Base name of the backup copy file as it's stored
        on the destination.
    :type name: str
    """

    def __init__(self, host, run_type, name):
        super(PeriodicCopy, self).__init__(host, name)
        if run_type in INTERVALS:
            self._run_type = run_type
        else:
            raise WrongInputData(
                'Wrong value of run_type: %s. Must be one of %s'
                % (run_type, ", ".join(INTERVALS))
            )

    @property
    def run_type(self):
        """What run type was when the backup copy was taken."""
        return self._run_type

    @property
    def _extra_path(self):
        return self.run_type
