from twindb_backup.copy.mysql_copy import MySQLCopy


def test_as_dict():
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    assert copy.as_dict() == {
        "type": "full",
        "backup_finished": None,
        "backup_started": None,
        "binlog": None,
        "config": {},
        "host": "foo",
        "lsn": None,
        "name": "some_file.txt",
        "parent": None,
        "position": None,
        "run_type": "daily",
        "galera": False,
        "wsrep_provider_version": None
    }
