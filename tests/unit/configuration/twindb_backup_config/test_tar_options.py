from textwrap import dedent

import pytest

from twindb_backup.configuration import ConfigurationError, TwinDBBackupConfig


def test_no_tar_options(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.tar_options is None


def test_tar_options(tmpdir):
    cfg_file = tmpdir.join("twindb-backup.cfg")
    with open(str(cfg_file), "w") as fp:
        fp.write(
            dedent(
                """
                [source]
                tar_options = --exclude-vcs-ignores
                """
            )
        )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.tar_options == "--exclude-vcs-ignores"
