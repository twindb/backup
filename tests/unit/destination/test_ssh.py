import logging
from subprocess import PIPE
import mock
import pytest
from twindb_backup import setup_logging
from twindb_backup.destination.base_destination import DestinationError
from twindb_backup.destination.ssh import Ssh

log = logging.getLogger('twindb_backup')
setup_logging(log, debug=True)


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
@mock.patch.object(Ssh, '_execute_commnand')
def test__status_exists(mock_run, out, result):
    mock_stdout = mock.Mock()
    mock_stdout.channel.recv_exit_status.return_value = 0
    mock_stdout.read.return_value = out
    mock_stderr = mock.Mock()
    mock_run.return_value = (mock_stdout, mock_stderr)
    dst = Ssh(remote_path='/foo/bar')
    assert dst._status_exists() == result

@mock.patch.object(Ssh, '_execute_commnand')
def test__status_exists_raises_error(mock_run):
    mock_stdout = mock.Mock()
    mock_stdout.channel.recv_exit_status.return_value = 0
    mock_stdout.read.return_value = 'foo'
    mock_stderr = mock.Mock()

    mock_run.return_value = (mock_stdout, mock_stderr)
    dst = Ssh(remote_path='/foo/bar')
    with pytest.raises(DestinationError):
        dst._status_exists()

@mock.patch.object(Ssh, '_status_exists')
def test_get_status_empty(mock_status_exists):
    mock_status_exists.return_value = False

    dst = Ssh(remote_path='/foo/bar')
    assert dst.status() == {
        'hourly': {},
        'daily': {},
        'weekly': {},
        'monthly': {},
        'yearly': {}
    }


def test_basename():
    dst = Ssh(remote_path='/foo/bar')
    assert dst.basename('/foo/bar/some_dir/some_file.txt') \
        == 'some_dir/some_file.txt'


@mock.patch.object(Ssh, '_get_remote_stdout')
def test_find_files(mock_run_command):

    dst = Ssh(remote_path='/foo/bar')
    dst.find_files('/foo/bar', 'abc')
    mock_run_command.assert_called_once_with(['find /foo/bar/*/abc -type f'])
