from textwrap import dedent

from twindb_backup.configuration import TwinDBBackupConfig


def test_ssh(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.ssh.user == 'root'
    assert tbc.ssh.key == '/root/.ssh/id_rsa'
    assert tbc.ssh.port == 123
    assert tbc.ssh.host == '127.0.0.1'
    assert tbc.ssh.path == '/tmp/backup'


def test_no_ssh_section(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write('')
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.ssh is None


def test_default_values(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write(
            dedent(
                """
                [ssh]
                ssh_user=root
                ssh_key=/etc/twindb/private_key

                [mysql]
                mysql_defaults_file=/etc/twindb/my.cnf
                """
            )
        )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.ssh.host == '127.0.0.1'
    assert tbc.ssh.path == '/var/backup'
