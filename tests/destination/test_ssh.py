import logging
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
@mock.patch('twindb_backup.destination.ssh.Popen')
def test__status_exists(mock_popen, out, result):
    mock_proc = mock.Mock()
    mock_proc.communicate.return_value = (out, '')
    mock_proc.returncode = 0

    mock_popen.return_value = mock_proc
    dst = Ssh(remote_path='/foo/bar')
    assert dst._status_exists() == result


@mock.patch('twindb_backup.destination.ssh.Popen')
def test__status_exists_raises_error(mock_popen):
    mock_proc = mock.Mock()
    mock_proc.communicate.return_value = ('foo', '')
    mock_proc.returncode = 0

    mock_popen.return_value = mock_proc
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
