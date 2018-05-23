from twindb_backup.status.binlog_status import BinlogStatus


def test_init_not_empty(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    assert instance.version == 0
    assert instance.valid


def test_init_empty():
    instance = BinlogStatus()
    assert instance.valid

