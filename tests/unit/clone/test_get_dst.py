import mock
import pytest

from twindb_backup.clone import get_dst
from twindb_backup.configuration import SSHConfig, TwinDBBackupConfig
from twindb_backup.destination.ssh import Ssh


@pytest.mark.parametrize(
    "destination, host, user, key",
    [("slave:3306", "slave", "foo", "bar"), ("slave", "slave", "foo", "bar")],
)
def test_get_dst(destination, host, user, key):
    with mock.patch.object(
        Ssh, "__init__", return_value=None
    ) as mock_dst, mock.patch.object(
        TwinDBBackupConfig,
        "ssh",
        new_callable=mock.PropertyMock,
        return_value=SSHConfig(ssh_user=user, ssh_key=key),
    ):
        get_dst(TwinDBBackupConfig(), destination)
        mock_dst.assert_called_once_with(
            "/tmp", ssh_host=host, ssh_user=user, ssh_key=key
        )
