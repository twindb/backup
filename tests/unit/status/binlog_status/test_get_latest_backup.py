from twindb_backup.status.binlog_status import BinlogStatus


def test_get_latest_backup(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    assert instance.get_latest_backup() == "master1/binlog/mysqlbin005.bin"
