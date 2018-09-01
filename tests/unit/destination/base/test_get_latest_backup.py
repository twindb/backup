import mock
import pytest

from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.status.mysql_status import MySQLStatus

@pytest.mark.parametrize('remote_path', [
    (
        's3://bucket'
    ),
    (
        '/path/to/twindb-server-backups'
    )
])
@mock.patch.object(BaseDestination, 'status')
def test_get_latest_backup_return_valid_url_path(mock_status, remote_path, status_raw_content):
    dst = BaseDestination(remote_path)
    mock_status.return_value = MySQLStatus(content=status_raw_content)
    url = dst.get_latest_backup()
    expected = "%s/master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz" % remote_path
    assert url == expected
