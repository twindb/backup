from twindb_backup.copy.mysql_copy import MySQLCopy


def test_key():
    backup_copy = MySQLCopy("foo", "daily", "some_file.txt", type="full")

    assert backup_copy.key == "foo/daily/mysql/some_file.txt"
