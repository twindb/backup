# noinspection PyPackageRequirements
import mock
# noinspection PyPackageRequirements
import pytest

from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.destination.ssh import Ssh
from twindb_backup.ssh.client import SshClient


# # noinspection PyUnresolvedReferences
# @pytest.mark.parametrize('out, result', [
#     (
#         'exists',
#         True
#     ),
#     (
#         'not_exists',
#         False
#     ),
#     (
#         'exists\n',
#         True
#     ),
#     (
#         'not_exists\n',
#         False
#     )
# ])
# @mock.patch.object(SshClient, "execute")
# def test__status_exists(mock_client, out, result):
#     mock_stdout = mock.Mock()
#     mock_stdout.read.return_value = out
#     mock_client.return_value = iter(
#         (
#             mock_stdout,
#             mock.Mock()
#         )
#     )
#     dst = Ssh(remote_path='/foo/bar')
#     # noinspection PyProtectedMember
#     assert dst._status_exists() == result
#
#
# # noinspection PyUnresolvedReferences
# @mock.patch.object(SshClient, "execute")
# def test__status_exists_raises_error(mock_run):
#     mock_stdout = mock.Mock()
#     mock_stdout.channel.recv_exit_status.return_value = 0
#     mock_stdout.read.return_value = 'foo'
#
#     mock_run.return_value = iter(
#         (
#             mock_stdout,
#             mock.Mock()
#         )
#     )
#
#     dst = Ssh(remote_path='/foo/bar')
#     with pytest.raises(SshDestinationError):
#         # noinspection PyProtectedMember
#         dst._status_exists()
