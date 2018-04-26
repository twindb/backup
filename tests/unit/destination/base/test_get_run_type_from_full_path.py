import pytest

from twindb_backup.destination.base_destination import BaseDestination


@pytest.mark.parametrize('remote_path, path, run_type', [
    (
        's3://bucket',
        's3://bucket/ip-10-0-52-101/daily/files/_etc-2018-04-05_00_07_13.tar.gz',
        'daily'
    ),
    (
        '/path/to/twindb-server-backups/',
        '/path/to/twindb-server-backups/master1/hourly/mysql/mysql-2018-04-08_03_51_18.xbstream.gz',
        'hourly'
    )
])
def test__get_run_type_from_full_path(remote_path, path, run_type):
    dst = BaseDestination(remote_path)
    assert dst.get_run_type_from_full_path(path) == run_type
