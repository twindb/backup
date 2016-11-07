from ConfigParser import ConfigParser
import mock as mock
import pytest
import time
from twindb_backup import delete_local_files, get_directories_to_backup, \
    get_timeout
from twindb_backup.backup import run_backup_job

__author__ = 'aleks'


@pytest.mark.parametrize('keep, calls', [
    (
        1,
        [mock.call('aaa'), mock.call('bbb')]
    ),
    (
        2,
        [mock.call('aaa')]
    ),
    (
        3,
        []
    ),
    (
        0,
        [mock.call('aaa'), mock.call('bbb'), mock.call('ccc')]
    )
])
@mock.patch('twindb_backup.os')
@mock.patch('twindb_backup.glob')
def test_delete_local_files(mock_glob, mock_os, keep, calls):
    mock_glob.glob.return_value = ['aaa', 'bbb', 'ccc']

    delete_local_files('/foo', keep)
    mock_os.unlink.assert_has_calls(calls)


@pytest.mark.parametrize('config_text, dirs', [
    (
        """
[source]
backup_dirs=/etc /root /home
        """,
        ['/etc', '/root', '/home']

    ),
    (
        """
[source]
backup_dirs="/etc /root /home"
        """,
        ['/etc', '/root', '/home']

    ),
    (
        """
[source]
backup_dirs='/etc /root /home'
        """,
        ['/etc', '/root', '/home']

    ),
    (
        """
[source]
        """,
        []

    ),
    (
        """
        """,
        []

    )
])
def test_get_directories_to_backup(config_text, dirs, tmpdir):
    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_text)
    cparser = ConfigParser()
    cparser.read(str(config_file))
    assert get_directories_to_backup(cparser) == dirs


@pytest.mark.parametrize('run_type, timeout', [
    (
        'hourly',
        3600 / 2
    ),
    (
        'daily',
        24 * 3600 / 2
    ),
    (
        'weekly',
        7 * 24 * 3600 / 2
    ),
    (
        'monthly',
        30 * 24 * 3600 / 2
    ),
    (
        'yearly',
        365 * 24 * 3600 / 2
    )
])
def test_get_timeout(run_type, timeout):
    assert get_timeout(run_type) == timeout


@mock.patch('twindb_backup.backup.backup_everything')
@mock.patch('twindb_backup.backup.get_timeout')
def test_run_backup_job_gets_lock(mock_get_timeout, mock_backup_everything, tmpdir):
    config_content = """
[source]
backup_dirs=/etc /root /home

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=yes
run_yearly=yes
    """
    lock_file = str(tmpdir.join('foo.lock'))
    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_content)

    cparser = ConfigParser()
    cparser.read(str(config_file))

    mock_get_timeout.return_value = 1

    run_backup_job(cparser, 'hourly', lock_file=lock_file)
    mock_backup_everything.assert_called_once_with('hourly', cparser)
