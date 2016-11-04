from ConfigParser import ConfigParser, NoOptionError
import mock as mock
import pytest
from twindb_backup import delete_local_files, get_directories_to_backup

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
