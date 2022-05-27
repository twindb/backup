import logging

import mock

from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


@mock.patch.object(MySQLSource, "get_connection")
def test__enable_wsrep_desync_sets_wsrep_desync_to_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    source.enable_wsrep_desync()

    mock_cursor.execute.assert_called_with("SET GLOBAL wsrep_desync=ON")
