from twindb_backup.copy.binlog_copy import BinlogCopy


def test_two_eq():
    copy1 = BinlogCopy('foo', 'bar', 10)
    copy2 = BinlogCopy('foo', 'bar', 10)
    assert copy1 == copy2


def test_two_neq_created():
    copy1 = BinlogCopy('foo', 'bar', 10)
    copy2 = BinlogCopy('foo', 'bar', 20)
    assert copy1 != copy2


def test_two_neq_name():
    copy1 = BinlogCopy('foo', 'bar1', 10)
    copy2 = BinlogCopy('foo', 'bar2', 10)
    assert copy1 != copy2
