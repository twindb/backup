import mock
import pytest
from pymysql import OperationalError

from twindb_backup.source.exceptions import MySQLSourceError
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


@mock.patch("twindb_backup.source.mysql_source.pymysql.connect")
def test__get_connection_raises_mysql_source_error(mock_connect):
    mock_connect.side_effect = OperationalError
    source = MySQLSource(MySQLConnectInfo(None, hostname=None), "daily", "full")
    with pytest.raises(MySQLSourceError):
        with source.get_connection():
            pass
