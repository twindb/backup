# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh

# noinspection PyUnresolvedReferences
# @mock.patch.object(Ssh, '__init__')
# def test_find_files(mock_init):
#     mock_init.return_value = mock_init
#
#     dst = Ssh(remote_path='/foo/bar')
#     dst.find_files('/foo/bar', 'abc')
#     mock_get_remote_handlers.return_value = None, None, None
#     mock_get_remote_handlers.assert_called_once_with(
#         'find /foo/bar/*/abc -type f'
#     )
