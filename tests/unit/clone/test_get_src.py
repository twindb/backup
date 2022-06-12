import mock

from twindb_backup import INTERVALS
from twindb_backup.clone import get_src
from twindb_backup.configuration import (
    MySQLConfig,
    SSHConfig,
    TwinDBBackupConfig,
)
from twindb_backup.source.mysql_source import (
    MySQLClient,
    MySQLConnectInfo,
    MySQLFlavor,
)


def test_get_src():
    with mock.patch(
        "twindb_backup.clone.get_src_by_vendor"
    ) as mock_get_src_by_vendor, mock.patch.object(
        TwinDBBackupConfig,
        "ssh",
        new_callable=mock.PropertyMock,
        return_value=SSHConfig(ssh_user="foo_user", ssh_key="foo_key"),
    ), mock.patch.object(
        TwinDBBackupConfig,
        "mysql",
        new_callable=mock.PropertyMock,
        return_value=MySQLConfig(mysql_defaults_file="foo_path"),
    ), mock.patch.object(
        MySQLClient,
        "server_vendor",
        new_callable=mock.PropertyMock,
        return_value=MySQLFlavor.PERCONA,
    ):
        get_src(TwinDBBackupConfig(), MySQLClient("foo_path"), "master:3306")
        mock_get_src_by_vendor.assert_called_once_with(
            MySQLFlavor.PERCONA,
            "master",
            "foo_user",
            "foo_key",
            MySQLConnectInfo("foo_path", hostname="master"),
            INTERVALS[0],
        )
