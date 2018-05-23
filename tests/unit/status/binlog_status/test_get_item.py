from twindb_backup.status.binlog_status import BinlogStatus


def test_get_item_existed_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    copy = instance["master1/binlog/mysqlbin001.bin"]
    assert copy is not None


def test_get_item_not_existed_copy(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    copy = instance["foo/bar"]
    assert copy is None
