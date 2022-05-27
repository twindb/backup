from twindb_backup.status.binlog_status import BinlogStatus


def test_init_not_empty(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    assert instance.version == 1
    assert len(instance) == 5


def test_init_empty():
    instance = BinlogStatus()
    assert len(instance) == 0


def test_init_restores_name(raw_binlog_status):
    status = BinlogStatus(raw_binlog_status)
    copy = status["master1/binlog/mysqlbin001.bin"]
    assert copy.name == "mysqlbin001.bin"
