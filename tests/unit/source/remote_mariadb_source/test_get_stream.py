import pytest
from mock.mock import Mock

from twindb_backup import INTERVALS
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mariadb_source import RemoteMariaDBSource


def test_get_stream():
    with pytest.raises(NotImplementedError):
        RemoteMariaDBSource(
            {
                "mysql_connect_info": Mock(spec=MySQLConnectInfo),
                "run_type": INTERVALS[0],
                "backup_type": "full",
            }
        ).get_stream()
