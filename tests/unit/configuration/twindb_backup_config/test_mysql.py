from textwrap import dedent

from twindb_backup.configuration import TwinDBBackupConfig


def test_mysql(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.mysql.full_backup == 'daily'
    assert tbc.mysql.defaults_file == '/etc/twindb/my.cnf'
    assert tbc.mysql.expire_log_days == 8
    assert tbc.mysql.xtrabackup_binary == \
        '/opt/twindb-backup/embedded/bin/xtrabackup'
    assert tbc.mysql.xbstream_binary == \
        '/opt/twindb-backup/embedded/bin/xbstream'


def test_no_mysql_section(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write('')
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.mysql is None


def test_mysql_set_xtrabackup_binary(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    tbc.mysql.xtrabackup_binary = 'foo'
    assert tbc.mysql.xtrabackup_binary == 'foo'


def test_mysql_set_xbstream_binary(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    tbc.mysql.xbstream_binary= 'foo'
    assert tbc.mysql.xbstream_binary == 'foo'


def test_custom_xtrabackup_binary(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write(
            dedent(
                """
                [mysql]
                mysql_defaults_file=/etc/twindb/my.cnf
                expire_log_days = 8
                xtrabackup_binary = foo
                xbstream_binary = bar
                """
            )
        )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.mysql.xtrabackup_binary == 'foo'
    assert tbc.mysql.xbstream_binary == 'bar'
