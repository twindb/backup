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
