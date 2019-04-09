import pytest

from twindb_backup.copy.exceptions import WrongInputData
from twindb_backup.copy.periodic_copy import PeriodicCopy


@pytest.mark.parametrize('path, host, run_type, name', [
    (
        '/foo/bar/hourly/mysql/mysql.tar.gz',
        'bar',
        'hourly',
        'mysql.tar.gz'
    ),
    (
        's3://twindb-www.twindb.com/master1/hourly/mysql/mysql-2016-11-23_21_50_54.xbstream.gz',
        'master1',
        'hourly',
        'mysql-2016-11-23_21_50_54.xbstream.gz'
    ),
    (
        's3://twindb-www.twindb.com/master.box/hourly/mysql/mysql-2016-11-23_08_01_25.xbstream.gz',
        'master.box',
        'hourly',
        'mysql-2016-11-23_08_01_25.xbstream.gz'
    ),
    (
        '/path/to/twindb-server-backups/master1/daily/mysql/mysql-2016-11-23_21_47_21.xbstream.gz',
        'master1',
        'daily',
        'mysql-2016-11-23_21_47_21.xbstream.gz'
    )
])
def test_copy_from_path(path, host, run_type, name):
    copy = PeriodicCopy(path=path)
    assert copy.host == host
    assert copy.run_type == run_type
    assert copy.name == name


def test_init_raises_on_wrong_inputs():
    with pytest.raises(WrongInputData):
        PeriodicCopy(path='foo')
