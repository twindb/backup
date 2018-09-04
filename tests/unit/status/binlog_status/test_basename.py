from twindb_backup.status.binlog_status import BinlogStatus


def test_basename():
    assert BinlogStatus().basename == 'binlog-status'
