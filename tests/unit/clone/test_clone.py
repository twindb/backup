import mock

from twindb_backup.clone import clone_mysql
from twindb_backup.source.mysql_source import MySQLFlavor
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


def test_clone():
    mock_cfg = mock.Mock()
    mock_mysql_client = mock.Mock()
    mock_mysql_client.server_vendor = MySQLFlavor.PERCONA
    with mock.patch(
        "twindb_backup.clone.MySQLClient", return_value=mock_mysql_client
    ), mock.patch("twindb_backup.clone.get_src") as mock_get_src, mock.patch(
        "twindb_backup.clone.detect_xbstream"
    ) as mock_detect_xbstream, mock.patch(
        "twindb_backup.clone.get_dst"
    ), mock.patch(
        "twindb_backup.clone.step_ensure_empty_directory"
    ), mock.patch(
        "twindb_backup.clone.step_run_remote_netcat"
    ), mock.patch(
        "twindb_backup.clone.step_clone_source"
    ), mock.patch(
        "twindb_backup.clone.step_clone_mysql_config"
    ), mock.patch(
        "twindb_backup.clone.step_start_mysql_service"
    ), mock.patch(
        "twindb_backup.clone.step_configure_replication"
    ), mock.patch.object(
        RemoteMySQLSource, "apply_backup", return_value=("bin.001", 123)
    ):
        clone_mysql(
            mock_cfg, "master1:3306", "slave1:3307", "foo_user", "foo_pass"
        )
        mock_get_src.assert_called_once_with(
            mock_cfg, mock_mysql_client, "master1:3306"
        )
        mock_detect_xbstream.assert_called_once_with(
            mock_cfg, mock_mysql_client
        )
