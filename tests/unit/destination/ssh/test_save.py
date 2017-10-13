# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh


# noinspection PyUnresolvedReferences
@mock.patch.object(Ssh, 'execute_command')
@mock.patch.object(Ssh, '_mkdirname_r')
def test_save(mock_mkdirname_r, mock_execute):

    mock_stdout = mock.Mock()
    mock_stdout.channel.recv_exit_status.return_value = 0

    mock_execute.return_value = mock_stdout, mock.Mock()

    dst = Ssh(remote_path='/path/to/backups')
    dst.save('foo', 'aaa/bbb/ccc/bar')

    mock_mkdirname_r.assert_called_once_with('/path/to/backups/aaa/bbb/ccc/bar')
    mock_execute.asser_called_once_with([
        'cat - > "/path/to/backups/aaa/bbb/ccc/bar"'
    ])
