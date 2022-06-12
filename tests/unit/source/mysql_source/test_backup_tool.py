import pytest

from twindb_backup import XTRABACKUP_BINARY
from twindb_backup.source.mysql_source import MySQLConnectInfo, MySQLSource


@pytest.mark.parametrize(
    "kwargs, expected_tool",
    [
        ({}, XTRABACKUP_BINARY),
        ({"xtrabackup_binary": None}, XTRABACKUP_BINARY),
        ({"xtrabackup_binary": "foo"}, "foo"),
    ],
)
def test_backup_tool(kwargs, expected_tool):
    ms = MySQLSource(MySQLConnectInfo("foo"), "daily", "full", **kwargs)
    assert ms.backup_tool == expected_tool
