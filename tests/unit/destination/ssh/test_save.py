# noinspection PyPackageRequirements
import mock
import pytest

from twindb_backup.destination.ssh import Ssh
from twindb_backup.ssh.client import SshClient
from twindb_backup.ssh.exceptions import SshClientException


# noinspection PyUnresolvedReferences
@mock.patch.object(SshClient, "get_remote_handlers")
@mock.patch.object(Ssh, '_mkdir_r')
def test_save(mock_mkdir_r, mock_execute):

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
    mock_handler = mock.MagicMock()
    mock_file_obj = mock.MagicMock()
    mock_file_obj.read.return_value = None
    mock_handler.__enter__.return_value = mock_file_obj
    mock_cin.read.return_value = 'foo'
    dst.save(mock_handler, 'aaa/bbb/ccc/bar')
    mock_execute.assert_called_once_with('cat - > /path/to/backups/aaa/bbb/ccc/bar')
    mock_handler.read_assert_called_once()
    mock_mkdir_r.assert_called_once_with('/path/to/backups/aaa/bbb/ccc')


# noinspection PyUnresolvedReferences
@mock.patch.object(SshClient, "get_remote_handlers")
@mock.patch.object(Ssh, '_mkdir_r')
def test_save_exception_not_handled(mock_mkdir_r, mock_execute):

    mock_cin = mock.Mock()
    mock_cin.channel.recv_exit_status.return_value = 0
    dst = Ssh(remote_path='/path/to/backups')
    mock_execute.side_effect = SshClientException
    with pytest.raises(SshClientException):
        dst.save(mock.MagicMock(), 'aaa/bbb/ccc/bar')
    mock_mkdir_r.assert_called_once_with('/path/to/backups/aaa/bbb/ccc')


@pytest.mark.parametrize('remote_path, name, remote_dirname', [
    (
        '/path/to/backups',
        'aaa/bbb/ccc/bar',
        '/path/to/backups/aaa/bbb/ccc'
    )
])
@mock.patch.object(SshClient, "get_remote_handlers")
@mock.patch.object(Ssh, '_mkdir_r')
def test_save_creates_remote_dirname(mock_mkdir_r, mock_execute, remote_path, name, remote_dirname):
    mock_cin = mock.Mock()
    mock_cin.channel.recv_exit_status.return_value = 0

    mock_execute.return_value.__enter__.return_value = iter(
        (
            mock_cin,
            None,
            None
        )
    )
    dst = Ssh(remote_path=remote_path)
    mock_handler = mock.MagicMock()
    mock_file_obj = mock.MagicMock()
    mock_file_obj.read.return_value = None
    mock_handler.__enter__.return_value = mock_file_obj
    mock_cin.read.return_value = 'foo'

    dst.save(mock_handler, 'aaa/bbb/ccc/bar')

    mock_mkdir_r.assert_called_once_with('/path/to/backups/aaa/bbb/ccc')
