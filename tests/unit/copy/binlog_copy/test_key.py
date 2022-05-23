from twindb_backup.copy.binlog_copy import BinlogCopy


def test_key():
    backup_copy = BinlogCopy("foo", "some_name", 100)

    assert backup_copy.key == "foo/binlog/some_name"
