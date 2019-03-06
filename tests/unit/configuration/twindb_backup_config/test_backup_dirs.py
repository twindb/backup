from textwrap import dedent

import pytest
from twindb_backup.configuration import TwinDBBackupConfig, ConfigurationError


def test_backup_dirs(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.backup_dirs == [
        '/', '/root/', '/etc', '/dir with space/', '/dir foo'
    ]


def test_no_backup_dirs(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write(
            dedent(
                """
                [source]
                """
            )
        )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.backup_dirs == []


def test_no_source(tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write('')
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    with pytest.raises(ConfigurationError):
        assert tbc.backup_dirs == []
