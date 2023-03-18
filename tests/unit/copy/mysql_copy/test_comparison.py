from twindb_backup.copy.mysql_copy import MySQLCopy


def test_lt():
    assert MySQLCopy("foo", "daily", "some_file.txt", type="full", lsn=1) < MySQLCopy(
        "foo", "daily", "some_file.txt", type="full", lsn=2
    )


def test_le():
    assert MySQLCopy("foo", "daily", "some_file.txt", type="full", lsn=1) <= MySQLCopy(
        "foo", "daily", "some_file.txt", type="full", lsn=1
    )


def test_gt():
    assert MySQLCopy("foo", "daily", "some_file.txt", type="full", lsn=2) > MySQLCopy(
        "foo", "daily", "some_file.txt", type="full", lsn=1
    )
