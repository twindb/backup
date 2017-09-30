import logging
from subprocess import PIPE
import mock
import pytest
from spur import SshShell

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
@mock.patch.object(SshShell, 'run')
def test__status_exists(mock_run, out, result):
    mock_proc = mock.Mock()
    mock_proc.returncode = 0
    mock_proc.output = out
    mock_run.return_value = mock_proc
    dst = Ssh(remote_path='/foo/bar')
    assert dst._status_exists() == result

@mock.patch.object(SshShell, 'run')
def test__status_exists_raises_error(mock_run):
    mock_proc = mock.Mock()
    mock_proc.output = 'foo'
    mock_proc.returncode = 0

    mock_run.return_value = mock_proc
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


@mock.patch('twindb_backup.destination.ssh.run_command')
def test_find_files(mock_run_command):

    dst = Ssh(remote_path='/foo/bar')
    dst.find_files('/foo/bar', 'abc')
    mock_run_command.assert_called_once_with([
        'ssh',
        '-l', 'root',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'PasswordAuthentication=no',
        '-p', '22',
        '-i', '/root/.id_rsa',
        '127.0.0.1',
        'find /foo/bar/*/abc -type f'],
        ok_non_zero=True
    )
