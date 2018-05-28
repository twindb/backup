import mock
import pytest

from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.status.mysql_status import MySQLStatus


@pytest.mark.parametrize('remote_path', [
        's3://bucket',
        '/path/to/twindb-server-backups',
])
@mock.patch.object(BaseDestination, 'status')
def test_get_latest_backup_with_filled_status(mocked_status, remote_path, filled_mysql_status):
    mocked_status.return_value = filled_mysql_status
    dst = BaseDestination(remote_path)
    assert remote_path + '/master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz' == dst.get_latest_backup()


@pytest.mark.parametrize('remote_path', [
    's3://bucket',
    '/path/to/twindb-server-backups/',
])
@mock.patch.object(BaseDestination, 'status')
def test_get_latest_backup_with_empty_status(mocked_status, remote_path):
    mocked_status.return_value = MySQLStatus()
    dst = BaseDestination(remote_path)
    assert dst.get_latest_backup() is None
