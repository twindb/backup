import pytest

from twindb_backup.util import get_hostname_from_backup_copy, get_run_type_from_backup_copy


@pytest.mark.parametrize('name, host', [
    (
        '/foo/bar/hourly/mysql/mysql.tar.gz',
        'hourly'
    ),
    (
        's3://twindb-www.twindb.com/master1/hourly/mysql/mysql-2016-11-23_21_50_54.xbstream.gz',
        'hourly'
    ),
    (
        's3://twindb-www.twindb.com/master.box/hourly/mysql/mysql-2016-11-23_08_01_25.xbstream.gz',
        'hourly'
    ),
    (
        '/path/to/twindb-server-backups/master1/daily/mysql/mysql-2016-11-23_21_47_21.xbstream.gz',
        'daily'
    )
])
def test_get_run_type_from_backup_copy(name, host):
    assert get_run_type_from_backup_copy(name) == host
