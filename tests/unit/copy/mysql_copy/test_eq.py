from twindb_backup.copy.mysql_copy import MySQLCopy


def test_eq():
    copy_1 = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    copy_2 = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    copy_3 = MySQLCopy('bar', 'daily', 'some_file.txt', type='full')
    assert copy_1 == copy_2
    assert copy_1 != copy_3
