from twindb_backup.configuration.mysql import MySQLConfig


def test_mysql():
    mc = MySQLConfig()
    assert mc.defaults_file == '/root/.my.cnf'
    assert mc.full_backup == 'daily'
