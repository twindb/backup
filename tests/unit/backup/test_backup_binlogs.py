import mock

from twindb_backup.backup import _binlog_status_directory, backup_binlogs
from twindb_backup.source.mysql_source import MySQLClient
from twindb_backup.status.binlog_status import BinlogStatus


@mock.patch.object(BinlogStatus, "save")
@mock.patch("twindb_backup.backup.osp")
def test_backup_binlogs_returns_if_no_binlogs(mock_osp, mock_save):
    with mock.patch.object(MySQLClient, "variable", return_value=None):
        cfg = mock.Mock()
        # server_name is used for the BinlogStatus status_directory; it must
        # be a real string since it is joined with a filename.
        cfg.server_name = "test-host"
        backup_binlogs("foo", cfg)
        assert mock_osp.dirname.call_count == 0
        assert mock_save.call_count == 0


@mock.patch("twindb_backup.backup.socket")
def test_binlog_status_directory_is_per_host(mock_socket):
    """Binlog status lives under ``<server_name>/<hostname>`` so every
    replica tracks its own upload history independently.
    """
    mock_socket.gethostname.return_value = "replica-a"
    cfg = mock.Mock()
    cfg.server_name = "cluster-xyz"
    assert _binlog_status_directory(cfg) == "cluster-xyz/replica-a"
