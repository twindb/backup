import pytest

from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.exceptions import StatusKeyNotFound


def test_get_item_existing_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    copy = instance["master1/binlog/mysqlbin001.bin"]
    assert copy is not None


def test_get_item_not_existing_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    with pytest.raises(StatusKeyNotFound):
        # noinspection PyStatementEffect
        instance["foo/bar"]
