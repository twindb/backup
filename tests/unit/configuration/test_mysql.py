from twindb_backup.configuration.mysql import MySQLConfig


def test_mysql_defaults():
    mc = MySQLConfig()
    assert mc.defaults_file == "/root/.my.cnf"
    assert mc.full_backup == "daily"
    assert mc.expire_log_days == 7
    assert mc.xtrabackup_binary is None
    assert mc.xbstream_binary is None
    assert mc.hostname == "127.0.0.1"


def test_mysql_init():
    mc = MySQLConfig(
        mysql_defaults_file="/foo/bar",
        full_backup="weekly",
        expire_log_days=3,
        xtrabackup_binary="/foo/xtrabackup",
        xbstream_binary="/foo/xbstream",
        hostname="foo",
    )
    assert mc.defaults_file == "/foo/bar"
    assert mc.full_backup == "weekly"
    assert mc.expire_log_days == 3
    assert mc.xtrabackup_binary == "/foo/xtrabackup"
    assert mc.xbstream_binary == "/foo/xbstream"
    assert mc.hostname == "foo"


def test_mysql_set_xtrabackup_binary():
    mc = MySQLConfig()
    mc.xtrabackup_binary = "foo"
    assert mc.xtrabackup_binary == "foo"


def test_mysql_set_xbstream_binary():
    mc = MySQLConfig()
    mc.xbstream_binary = "foo"
    assert mc.xbstream_binary == "foo"
