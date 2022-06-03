# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh


# noinspection PyUnresolvedReferences
@mock.patch.object(Ssh, "execute_command")
def test_mkdir_r(mock_execute):

    mock_stdout = mock.Mock()
    mock_stdout.channel.recv_exit_status.return_value = 0

    mock_execute.return_value = mock_stdout, mock.Mock()

    dst = Ssh(remote_path="some_dir")
    # noinspection PyProtectedMember
    dst._mkdir_r("/foo/bar")
    mock_execute.assert_called_once_with('mkdir -p "/foo/bar"')
