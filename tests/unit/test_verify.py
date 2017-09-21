import mock

from twindb_backup import TwinDBBackupError
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.verify import verify_mysql_backup

@mock.patch('twindb_backup.verify.edit_backup_my_cnf')
@mock.patch('twindb_backup.verify.restore_from_mysql')
@mock.patch('twindb_backup.verify.get_destination')
def test_verify_mysql_backup_if_restore_is_success(mock_get_dst, mock_restore, mock_edit):
    mock_config = mock.Mock()
    mock_config.get.return_value = "/foo/bar"
    mock_dst = mock.MagicMock()
    mock_dst.get_latest_backup.return_value = "/foo/bar"
    mock_get_dst.return_value = mock_dst
    verify_mysql_backup(mock_config, '/foo/bar/', 'latest')
    mock_restore.assert_called_once()
    mock_edit.assert_called_once()


@mock.patch('twindb_backup.verify.edit_backup_my_cnf')
@mock.patch('twindb_backup.verify.restore_from_mysql')
@mock.patch('twindb_backup.verify.get_destination')
def test_verify_mysql_backup_if_restore_is_failed(mock_get_dst, mock_restore, mock_edit):
    mock_config = mock.Mock()
    mock_config.get.return_value = "/foo/bar"
    mock_dst = mock.MagicMock()
    mock_restore.side_effect = TwinDBBackupError

    mock_dst.get_latest_backup.return_value = "/foo/bar"
    mock_get_dst.return_value = mock_dst
    verify_mysql_backup(mock_config, '/foo/bar/', 'latest')
    mock_restore.assert_called_once()
    mock_edit.assert_not_called()

