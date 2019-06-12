import mock

from twindb_backup.backup import backup_binlogs
from twindb_backup.source.mysql_source import MySQLClient
from twindb_backup.status.binlog_status import BinlogStatus


@mock.patch.object(BinlogStatus, 'save')
@mock.patch('twindb_backup.backup.osp')
@mock.patch('twindb_backup.backup.binlogs_to_backup')
@mock.patch.object(MySQLClient, 'cursor')
def test_backup_binlogs_returns_if_no_binlogs(
        mock_cursor,
        mock_binlogs_to_backup,
        mock_osp,
        mock_save):

    mock_cursor_r = mock.MagicMock()
    mock_cursor_r.fetchone.return_value = {
        '@@log_bin_basename': None
    }

    mock_cursor.return_value.__enter__.return_value = mock_cursor_r

    backup_binlogs('foo', mock.Mock())
    mock_binlogs_to_backup.assert_called_once_with(
        mock_cursor_r, last_binlog=None
    )
    mock_cursor_r.execute.assert_has_calls(
        [
            mock.call('FLUSH BINARY LOGS'),
            mock.call('SELECT @@log_bin_basename'),
        ]
    )
    assert mock_osp.dirname.call_count == 0
    assert mock_save.call_count == 0
