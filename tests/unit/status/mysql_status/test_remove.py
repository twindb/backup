import pytest

from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.exceptions import StatusKeyNotFound, StatusError
from twindb_backup.status.mysql_status import MySQLStatus


def test_remove(status_raw_empty):
    status = MySQLStatus(status_raw_empty)
    assert status.valid
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    status.add(copy)
    assert len(status.daily) == 1
    status.remove(copy.key, copy.run_type)
    assert len(status.daily) == 0


def test_remove_raises(status_raw_empty):
    status = MySQLStatus(status_raw_empty)
    assert status.valid
    with pytest.raises(StatusKeyNotFound):
        status.remove("foo", "daily")


def test_remove_raises_when_wrong_period(status_raw_empty):
    status = MySQLStatus(status_raw_empty)
    assert status.valid
    with pytest.raises(StatusError):
        status.remove("minutely", "foo")
