import mock
import pytest

from twindb_backup import INTERVALS
from twindb_backup.clone import get_src_by_vendor
from twindb_backup.source.mysql_source import MySQLConnectInfo, MySQLFlavor
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


@pytest.mark.parametrize("vendor, expected_klass", [(MySQLFlavor.PERCONA, RemoteMySQLSource)])
def test_get_src_by_vendor(vendor, expected_klass):
    isinstance(
        get_src_by_vendor(
            vendor,
            "foo",
            "foo",
            "foo",
            mock.Mock(spec=MySQLConnectInfo),
            INTERVALS[0],
        ),
        expected_klass,
    )
