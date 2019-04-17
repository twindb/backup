from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.binlog_status import BinlogStatus


def test_get_latest_backup(raw_binlog_status):
    instance = BinlogStatus(raw_binlog_status)
    assert instance.latest_backup == BinlogCopy(
        host='master1',
        name='mysqlbin005.bin',
        created_at=100504
    )
