import mock

from twindb_backup.clone import step_configure_replication
from twindb_backup.source.mysql_source import MySQLMasterInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


def test_step_configure_replication():
    mock_dst = mock.Mock(spec=RemoteMySQLSource)
    step_configure_replication(
        mock_dst,
        MySQLMasterInfo("host_foo", 1234, "foo_user", "foo_password", "foo-1.1", 8873),
    )
