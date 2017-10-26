# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh
from twindb_backup.ssh.client import SshClient
from twindb_backup.ssh.exceptions import SshClientException


# noinspection PyUnresolvedReferences
@mock.patch.object(SshClient, "get_remote_handlers")
@mock.patch.object(Ssh, '_mkdirname_r')
def test_save(mock_mkdirname_r, mock_execute):

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
    mock_cin.read.return_value = 'foo'
    assert dst.save(mock_handler, 'aaa/bbb/ccc/bar')
    mock_execute.assert_called_once_with('cat - > /path/to/backups/aaa/bbb/ccc/bar')
    mock_cin.write.assert_called_once()
    mock_handler.read_assert_called_once()
    mock_mkdirname_r.assert_called_once_with('/path/to/backups/aaa/bbb/ccc/bar')


# noinspection PyUnresolvedReferences
@mock.patch.object(SshClient, "get_remote_handlers")
@mock.patch.object(Ssh, '_mkdirname_r')
def test_save_exception_return_false(mock_mkdirname_r, mock_execute):

    mock_cin = mock.Mock()
    mock_cin.channel.recv_exit_status.return_value = 0
    dst = Ssh(remote_path='/path/to/backups')
    mock_execute.side_effect = SshClientException
    assert not dst.save(mock.MagicMock(), 'aaa/bbb/ccc/bar')
    mock_mkdirname_r.assert_called_once_with('/path/to/backups/aaa/bbb/ccc/bar')
