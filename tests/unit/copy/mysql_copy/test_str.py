from twindb_backup.copy.mysql_copy import MySQLCopy


def test_repr():
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    assert repr(copy) == 'MySQLCopy(foo/daily/mysql/some_file.txt)'


def test_str():
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')

    expected = """MySQLCopy(foo/daily/mysql/some_file.txt) = {
    "backup_finished": null,
    "backup_started": null,
    "binlog": null,
    "config": {},
    "galera": false,
    "host": "foo",
    "lsn": null,
    "name": "some_file.txt",
    "parent": null,
    "position": null,
    "run_type": "daily",
    "type": "full",
    "wsrep_provider_version": null
}"""
    assert str(copy) == expected
