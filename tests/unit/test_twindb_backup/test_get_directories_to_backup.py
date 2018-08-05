from ConfigParser import ConfigParser

import pytest

from twindb_backup import get_directories_to_backup


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
