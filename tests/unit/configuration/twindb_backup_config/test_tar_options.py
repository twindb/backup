from textwrap import dedent

from twindb_backup.configuration import TwinDBBackupConfig


def test_no_tar_options(config_file):
    """Check config where there is no tar_options"""
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.tar_options is None


def test_tar_options(tmpdir):
    """Check tar_options becomes a property."""
    cfg_file = tmpdir.join("twindb-backup.cfg")
    with open(str(cfg_file), "w", encoding="utf-8") as config_desc:
        config_desc.write(
            dedent(
                """
                [source]
                tar_options = --exclude-vcs-ignores
                """
            )
        )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.tar_options == "--exclude-vcs-ignores"
