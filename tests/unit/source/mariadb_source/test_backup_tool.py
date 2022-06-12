import pytest

from twindb_backup import MARIABACKUP_BINARY
from twindb_backup.source.mariadb_source import MariaDBSource
from twindb_backup.source.mysql_source import MySQLConnectInfo


@pytest.mark.parametrize(
    "kwargs, expected_tool",
    [
        ({}, MARIABACKUP_BINARY),
        ({"xtrabackup_binary": None}, MARIABACKUP_BINARY),
        ({"xtrabackup_binary": "foo"}, "foo"),
    ],
)
def test_backup_tool(kwargs, expected_tool):
    ms = MariaDBSource(MySQLConnectInfo("foo"), "daily", "full", **kwargs)
    assert ms.backup_tool == expected_tool
