import ConfigParser
import StringIO

from mock import mock

from twindb_backup.backup import backup_mysql
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


@mock.patch("twindb_backup.backup.MySQLSource")
@mock.patch("twindb_backup.backup.prepare_status")
@mock.patch("twindb_backup.backup._backup_stream")
@mock.patch("twindb_backup.backup.get_destination")
def test_backup_mysql_status_updated(mock_get_dst,
                                     mock_backup_stream,
                                     mock_prep_status,
                                     mock_mysql_source,
                                     config_content):
    s_config = config_content.format(destination="ssh", port='1234')
    buf = StringIO.StringIO(s_config)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)
    mock_dst = mock.Mock()
    mock_get_dst.return_value = mock_dst
    backup_mysql("hourly", config)
    mock_dst.status.assert_called_once()
