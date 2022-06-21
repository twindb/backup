import pytest

from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.source.mysql_source import MySQLFlavor


@pytest.mark.parametrize(
    "vendor, binary",
    [
        (MySQLFlavor.ORACLE, "xtrabackup"),
        (MySQLFlavor.PERCONA, "xtrabackup"),
        (MySQLFlavor.MARIADB, "mariabackup"),
        ("mariadb", "mariabackup"),
        ("percona", "xtrabackup"),
        ("oracle", "xtrabackup"),
    ],
)
def test_xtrabackup_binary(vendor, binary):
    assert MySQLCopy("foo", "daily", "some_file.txt", type="full", server_vendor=vendor).xtrabackup_binary == binary
