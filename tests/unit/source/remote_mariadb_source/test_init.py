from mock.mock import Mock

from twindb_backup import INTERVALS, MARIABACKUP_BINARY
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mariadb_source import RemoteMariaDBSource


def test_init():
    assert (
        RemoteMariaDBSource(
            {
                "mysql_connect_info": Mock(spec=MySQLConnectInfo),
                "run_type": INTERVALS[0],
                "backup_type": "full",
            }
        ).backup_tool
        == MARIABACKUP_BINARY
    )
