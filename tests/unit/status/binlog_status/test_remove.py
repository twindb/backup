import pytest

from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.exceptions import StatusKeyNotFound


def test_remove_existed_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    instance.remove("master1/binlog/mysqlbin001.bin")
    assert "master1/binlog/mysqlbin001.bin" not in instance.copies


def test_remove_not_existed_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    with pytest.raises(StatusKeyNotFound):
        instance.remove("foo")
