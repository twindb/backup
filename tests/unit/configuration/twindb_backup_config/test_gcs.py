from twindb_backup.configuration import TwinDBBackupConfig


def test_gcs(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.gcs.gc_credentials_file == 'XXXXX'
    assert tbc.gcs.gc_encryption_key == ''
    assert tbc.gcs.bucket == 'twindb-backups'


def test_no_gcs_section(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write('')
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.gcs is None
