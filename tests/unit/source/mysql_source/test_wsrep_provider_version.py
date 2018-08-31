import logging

import mock

from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


@mock.patch.object(MySQLSource, 'get_connection')
def test__wsrep_provider_version_returns_correct_version(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchall.return_value = [
        {'Variable_name': 'wsrep_provider_version', 'Value': '3.19(rb98f92f)'},
    ]

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    assert source.wsrep_provider_version == '3.19'
