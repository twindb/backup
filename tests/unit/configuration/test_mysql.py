from twindb_backup.configuration.mysql import MySQLConfig


def test_mysql():
    mc = MySQLConfig()
    assert mc.defaults_file == '/root/.my.cnf'
    assert mc.full_backup == 'daily'


def test_mysql_set_xtrabackup_binary():
    mc = MySQLConfig()
    mc.xtrabackup_binary = 'foo'
    assert mc.xtrabackup_binary == 'foo'


def test_mysql_set_xbstream_binary():
    mc = MySQLConfig()
    mc.xbstream_binary = 'foo'
    assert mc.xbstream_binary == 'foo'
