import logging

import mock

from twindb_backup.source.mysql_source import MySQLConnectInfo, MySQLSource


@mock.patch.object(MySQLSource, "get_connection")
def test__disable_wsrep_desync_sets_wsrep_desync_to_off(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchall.return_value = [
        {"Variable_name": "wsrep_local_recv_queue", "Value": "0"},
    ]

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    source.disable_wsrep_desync()

    mock_cursor.execute.assert_any_call("SHOW GLOBAL STATUS LIKE " "'wsrep_local_recv_queue'")
    mock_cursor.execute.assert_called_with("SET GLOBAL wsrep_desync=OFF")
