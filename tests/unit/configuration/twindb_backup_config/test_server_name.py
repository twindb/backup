"""Tests for TwinDBBackupConfig.server_name."""

import socket
from textwrap import dedent

from twindb_backup.configuration import TwinDBBackupConfig


def _write_config(tmpdir, body):
    cfg_file = tmpdir.join("twindb-backup.cfg")
    with open(str(cfg_file), "w", encoding="utf-8") as fh:
        fh.write(dedent(body))
    return cfg_file


def test_server_name_defaults_to_hostname(tmpdir):
    """Without [source] server_name, we fall back to gethostname()."""
    cfg_file = _write_config(
        tmpdir,
        """
        [source]
        backup_mysql=no
        """,
    )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.server_name == socket.gethostname()


def test_server_name_from_config(tmpdir):
    cfg_file = _write_config(
        tmpdir,
        """
        [source]
        backup_mysql=no
        server_name=prod-primary-db
        """,
    )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.server_name == "prod-primary-db"


def test_server_name_strips_quotes_and_whitespace(tmpdir):
    cfg_file = _write_config(
        tmpdir,
        """
        [source]
        backup_mysql=no
        server_name = "  prod-primary-db  "
        """,
    )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.server_name == "prod-primary-db"


def test_server_name_empty_value_falls_back_to_hostname(tmpdir):
    cfg_file = _write_config(
        tmpdir,
        """
        [source]
        backup_mysql=no
        server_name =
        """,
    )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.server_name == socket.gethostname()


def test_server_name_without_source_section(tmpdir):
    """Missing [source] section must not crash server_name lookup."""
    cfg_file = _write_config(
        tmpdir,
        """
        [destination]
        backup_destination=s3
        """,
    )
    tbc = TwinDBBackupConfig(config_file=str(cfg_file))
    assert tbc.server_name == socket.gethostname()
