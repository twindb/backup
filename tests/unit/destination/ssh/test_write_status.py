# noinspection PyPackageRequirements
import mock
# noinspection PyPackageRequirements
import pytest

from twindb_backup.destination.exceptions import StatusFileError
from twindb_backup.destination.ssh import Ssh
from twindb_backup.ssh.client import SshClient

@mock.patch.object(Ssh, "_move_file")
@mock.patch.object(Ssh, "_is_valid_status")
@mock.patch.object(SshClient, "get_remote_handlers")
def test_write_status(mock_execute,
                      mock_valid_status,
                      mock_mv_file):
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
    mock_valid_status.return_value = True
    dst._write_status("{}")
    mock_execute.assert_called_once_with("cat - > %s" % dst.status_tmp_path)
    mock_cin.write.assert_called_once_with("{}")
    mock_mv_file.assert_called_once_with(dst.status_tmp_path, dst.status_path)


@mock.patch.object(Ssh, "_move_file")
@mock.patch.object(Ssh, "_is_valid_status")
@mock.patch.object(SshClient, "get_remote_handlers")
def test_write_status_if_status_is_invalid(mock_execute,
                                           mock_valid_status,
                                           mock_mv_file):
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
    mock_valid_status.return_value = False
    with pytest.raises(StatusFileError):
        dst._write_status("{}")
