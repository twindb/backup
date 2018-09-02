import mock
import pytest

from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.status.mysql_status import MySQLStatus


@pytest.mark.parametrize('remote_path, expected', [
    (
        's3://bucket',
        's3://bucket/master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz'
    ),
    (
        '/path/to/twindb-server-backups',
        '/path/to/twindb-server-backups/master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz'
    ),
    (
        '/path/to/twindb-server-backups/',
        '/path/to/twindb-server-backups/master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz'
    )
])
@mock.patch.object(BaseDestination, 'status')
def test_get_latest_backup_return_valid_url_path(mock_status, remote_path, expected, status_raw_content):
    dst = BaseDestination(remote_path)
    mock_status.return_value = MySQLStatus(content=status_raw_content)
    url = dst.get_latest_backup()
    assert url == expected
