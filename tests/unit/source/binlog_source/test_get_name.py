import mock

from twindb_backup.source.binlog_source import BinlogSource


@mock.patch("twindb_backup.source.base_source.socket.gethostname")
def test_get_name(mock_gethostname):

    mock_gethostname.return_value = "some-host"
    src = BinlogSource("daily", mock.Mock(), "foo-bin-log.0001")

    assert src.get_name() == "some-host/binlog/foo-bin-log.0001"
