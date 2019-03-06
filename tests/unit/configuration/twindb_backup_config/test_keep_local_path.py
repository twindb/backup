from textwrap import dedent

import pytest
from twindb_backup.configuration import TwinDBBackupConfig


def test_keep_local_path(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.keep_local_path == '/var/backup/local'


@pytest.mark.parametrize('content', [
    "",
    dedent(
        """
        [destination]
        """
    )
])
def test_no_keep_local_path(content, tmpdir):
    cfg_file = tmpdir.join('twindb-backup.cfg')
    with open(str(cfg_file), 'w') as fp:
        fp.write(content)
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.keep_local_path is None
