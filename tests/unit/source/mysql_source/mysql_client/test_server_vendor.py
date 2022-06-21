import mock
import pytest

from twindb_backup.source.mysql_source import MySQLClient, MySQLFlavor


@pytest.mark.parametrize(
    "version, version_comment, vendor",
    [
        (
            "10.3.34-MariaDB-0ubuntu0.20.04.1-log",
            "Ubuntu 20.04",
            MySQLFlavor.MARIADB,
        ),
        ("8.0.28", "MySQL Community Server - GPL", MySQLFlavor.ORACLE),
        (
            "8.0.28-19",
            "Percona Server (GPL), Release 19, Revision 31e88966cd3",
            MySQLFlavor.PERCONA,
        ),
    ],
)
def test_server_vendor(version, version_comment, vendor):
    with mock.patch.object(MySQLClient, "variable", side_effect=[version, version_comment]):
        assert MySQLClient("foo").server_vendor == vendor
