import json

from twindb_backup.copy.mysql_copy import MySQLCopy


def test_str():
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    string_view = str(copy)
    try:
        json_object = json.loads(string_view)
        assert True
    except ValueError, e:
        raise
