# noinspection PyPackageRequirements
import mock
# noinspection PyPackageRequirements
import pytest

from twindb_backup.destination.ssh import Ssh
from twindb_backup.ssh.client import SshClient

@mock.patch.object(SshClient, "get_remote_handlers")
def test_write_status(mock_execute):
    mock_cin = mock.Mock()
    mock_cin.channel.recv_exit_status.return_value = 0

    mock_execute.return_value.__enter__.return_value = iter(
        (
            mock_cin,
            None,
            None
        )
    )
    dst = Ssh(remote_path='/path/to/backups')
    dst._write_status("{}")
    mock_cin.write().asset_called_once_with({})
    mock_execute.asset_called_once_with("cat - > %s" % dst.status_path)
