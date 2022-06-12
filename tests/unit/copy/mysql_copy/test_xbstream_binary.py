import pytest

from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.source.mysql_source import MySQLFlavor


@pytest.mark.parametrize(
    "vendor, binary",
    [
        (MySQLFlavor.ORACLE, "xbstream"),
        (MySQLFlavor.PERCONA, "xbstream"),
        (MySQLFlavor.MARIADB, "mbstream"),
        ("mariadb", "mbstream"),
        ("percona", "xbstream"),
        ("oracle", "xbstream"),
    ],
)
def test_xbstream_binary(vendor, binary):
    assert (
        MySQLCopy(
            "foo", "daily", "some_file.txt", type="full", server_vendor=vendor
        ).xbstream_binary
        == binary
    )
