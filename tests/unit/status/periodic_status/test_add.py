import pytest

from twindb_backup import INTERVALS
from twindb_backup.copy.periodic_copy import PeriodicCopy
from twindb_backup.status.periodic_status import PeriodicStatus


@pytest.mark.parametrize('run_type', INTERVALS)
def test_add(run_type):
    status = PeriodicStatus()
    copy = PeriodicCopy('foo', run_type, 'bar')
    copy._source_type = 'type-foo'
    status.add(copy)
    assert getattr(status, run_type) == {
        copy.key: copy
    }
