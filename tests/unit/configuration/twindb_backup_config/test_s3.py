from twindb_backup.configuration import TwinDBBackupConfig


def test_s3(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.s3.aws_access_key_id == 'XXXXX'
    assert tbc.s3.aws_secret_access_key == 'YYYYY'
    assert tbc.s3.aws_default_region == 'us-east-1'
    assert tbc.s3.bucket == 'twindb-backups'


def test_no_s3_section(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write('')
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.s3 is None
