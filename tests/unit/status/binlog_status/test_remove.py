import pytest

from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.exceptions import StatusKeyNotFound


def test_remove_existing_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    copy = BinlogCopy(
        host='master1',
        name='mysqlbin001.bin',
        created_at=100500
    )
    assert copy in instance
    instance.remove("master1/binlog/mysqlbin001.bin")
    assert copy not in instance


def test_remove_non_existing_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    with pytest.raises(StatusKeyNotFound):
        instance.remove("foo")
