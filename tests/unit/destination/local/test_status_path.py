import mock
import pytest

from twindb_backup.destination.local import Local
from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.mysql_status import MySQLStatus


@pytest.mark.parametrize('cls, filename', [
    (
        MySQLStatus,
        'status'
    ),
    (
        BinlogStatus,
        'binlog-status'
    )
])
@mock.patch('twindb_backup.destination.local.socket')
def test_status_path_mysql(mock_socket, cls, filename, tmpdir):
    mock_socket.gethostname.return_value = 'foo'
    path = tmpdir.mkdir('backups')
    dst = Local(str(path))
    assert dst.status_path(cls=cls) == '%s/foo/%s' \
                                       % (str(path), filename)
