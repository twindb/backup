from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.binlog_status import BinlogStatus


def test_add_to_empty_status():
    instance = BinlogStatus()
    instance.add(BinlogCopy("host", "foo/bar", 100500))
    assert instance.valid
    assert "foo/bar" not in instance.copies
