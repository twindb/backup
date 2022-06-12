import mock

from twindb_backup.backup import backup_binlogs
from twindb_backup.source.mysql_source import MySQLClient
from twindb_backup.status.binlog_status import BinlogStatus


@mock.patch.object(BinlogStatus, "save")
@mock.patch("twindb_backup.backup.osp")
def test_backup_binlogs_returns_if_no_binlogs(mock_osp, mock_save):
    with mock.patch.object(MySQLClient, "variable", return_value=None):
        backup_binlogs("foo", mock.Mock())
        assert mock_osp.dirname.call_count == 0
        assert mock_save.call_count == 0
