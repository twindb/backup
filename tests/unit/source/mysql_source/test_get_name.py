import mock

from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


@mock.patch("twindb_backup.source.base_source.socket")
@mock.patch("twindb_backup.source.base_source.time")
def test_get_name(mock_time, mock_socket):

    host = "some-host"
    mock_socket.gethostname.return_value = host
    timestamp = "2017-02-13_15_40_29"
    mock_time.strftime.return_value = timestamp

    src = MySQLSource(
        MySQLConnectInfo("/foo/bar"), "daily", "full", dst=mock.Mock()
    )

    assert (
        src.get_name()
        == "some-host/daily/mysql/mysql-2017-02-13_15_40_29.xbstream"
    )
