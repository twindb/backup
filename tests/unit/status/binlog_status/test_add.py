from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.binlog_status import BinlogStatus


def test_add_to_empty_status():
    instance = BinlogStatus()

    instance.add(BinlogCopy("host", "foo/bar", 100500))
    assert BinlogCopy("host", "foo/bar", 100500) in instance

    instance.add(BinlogCopy("host", "foo/bar-2", 100501))
    assert BinlogCopy("host", "foo/bar", 100500) in instance
    assert BinlogCopy("host", "foo/bar-2", 100501) in instance
