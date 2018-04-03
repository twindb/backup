import pytest

from twindb_backup.status.exceptions import StatusKeyNotFound
from twindb_backup.status.status import Status


def test_remove(status_raw_empty):
    status = Status(status_raw_empty)
    assert status.valid
    status.add("daily", "foo")
    assert len(status.daily) == 1
    status.remove("daily", "foo")
    assert len(status.daily) == 0


def test_remove_raises(status_raw_empty):
    status = Status(status_raw_empty)
    assert status.valid
    with pytest.raises(StatusKeyNotFound):
        status.remove("daily", "foo")
