# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh


# noinspection PyUnresolvedReferences
@mock.patch.object(Ssh, '_get_remote_stdout')
def test_find_files(mock_run_command):

    dst = Ssh(remote_path='/foo/bar')
    dst.find_files('/foo/bar', 'abc')
    mock_run_command.assert_called_once_with(['find /foo/bar/*/abc -type f'])
