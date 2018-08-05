from twindb_backup.status.binlog_status import BinlogStatus


def test_eq(raw_binlog_status):
    status_1 = BinlogStatus(raw_binlog_status)
    status_2 = BinlogStatus(raw_binlog_status)

    assert status_1 == status_2


def test_ne(raw_binlog_status):
    status_1 = BinlogStatus(raw_binlog_status)
    status_2 = BinlogStatus()

    assert status_1 != status_2
