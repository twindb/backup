# noinspection PyPackageRequirements
import mock
# noinspection PyPackageRequirements
import pytest

from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.destination.ssh import Ssh
from twindb_backup.ssh.client import SshClient


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('out, result', [
    (
        'exists',
        True
    ),
    (
        'not_exists',
        False
    ),
    (
        'exists\n',
        True
    ),
    (
        'not_exists\n',
        False
    )
])
@mock.patch.object(SshClient, "get_remote_handlers")
def test__status_exists(mock_client, out, result):
    mock_stdout = mock.Mock()
    mock_stdout.read.return_value = out
    mock_client.return_value.__enter__.return_value = iter(
        (
            None,
            mock_stdout,
            None
        )
    )
    dst = Ssh(remote_path='/foo/bar')
    # noinspection PyProtectedMember
    assert dst._status_exists() == result


# noinspection PyUnresolvedReferences
@mock.patch.object(SshClient, "get_remote_handlers")
def test__status_exists_raises_error(mock_run):
    mock_stdout = mock.Mock()
    mock_stdout.channel.recv_exit_status.return_value = 0
    mock_stdout.read.return_value = 'foo'
    mock_stderr = mock.Mock()

    mock_run.return_value.__enter__.return_value = iter(
        (
            mock.Mock(),
            mock_stdout,
            mock_stderr
        )
    )

    dst = Ssh(remote_path='/foo/bar')
    with pytest.raises(SshDestinationError):
        # noinspection PyProtectedMember
        dst._status_exists()
