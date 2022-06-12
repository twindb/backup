import mock
import pytest

from twindb_backup import MBSTREAM_BINARY, XBSTREAM_BINARY
from twindb_backup.clone import detect_xbstream
from twindb_backup.configuration import MySQLConfig, TwinDBBackupConfig
from twindb_backup.source.mysql_source import MySQLClient, MySQLFlavor


@pytest.mark.parametrize(
    "given_xbstream, vendor, expected_xbstream",
    [
        ("foo", MySQLFlavor.MARIADB, "foo"),
        (None, MySQLFlavor.MARIADB, MBSTREAM_BINARY),
        (None, MySQLFlavor.PERCONA, XBSTREAM_BINARY),
        (None, MySQLFlavor.ORACLE, XBSTREAM_BINARY),
    ],
)
def test_detect_xbstream(given_xbstream, vendor, expected_xbstream):
    with mock.patch.object(
        MySQLClient,
        "server_vendor",
        new_callable=mock.PropertyMock,
        return_value=vendor,
    ), mock.patch.object(
        TwinDBBackupConfig,
        "mysql",
        new_callable=mock.PropertyMock,
        return_value=MySQLConfig(xbstream_binary=given_xbstream),
    ):
        assert (
            detect_xbstream(TwinDBBackupConfig(), MySQLClient("foo_path"))
            == expected_xbstream
        )
