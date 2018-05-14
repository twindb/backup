"""Interval class for a backup copy"""
from twindb_backup import INTERVALS
from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.copy.exceptions import WrongInputData


class IntervalCopy(BaseCopy):
    """Interval class for a backup copy in status."""

    def __init__(self, host, run_type, name):
        super(IntervalCopy, self).__init__(host, name)
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
