import pytest

from twindb_backup.destination.base_destination import BaseDestination


@pytest.mark.parametrize('remote_path, path, basename', [
    (
        's3://bucket',
        's3://bucket/ip-10-0-52-101/daily/files/_etc-2018-04-05_00_07_13.tar.gz',
        'ip-10-0-52-101/daily/files/_etc-2018-04-05_00_07_13.tar.gz'
    ),
    (
        '/path/to/twindb-server-backups/',
        '/path/to/twindb-server-backups/master1/hourly/mysql/mysql-2018-04-08_03_51_18.xbstream.gz',
        'master1/hourly/mysql/mysql-2018-04-08_03_51_18.xbstream.gz'
    )
])
def test_basename(remote_path, path, basename):
    dst = BaseDestination(remote_path)
    assert dst.basename(path) == basename
