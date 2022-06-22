import pytest

from twindb_backup.copy.exceptions import WrongInputData
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.source.mysql_source import MySQLFlavor


def test_init_raises_if_name_is_not_relative():
    with pytest.raises(WrongInputData):
        MySQLCopy("foo", "daily", "some/non/relative/path", type="full")


def test_init_has_config():
    copy = MySQLCopy("foo", "daily", "some_file.txt", type="full")
    assert copy.config == {}


def test_init_set_defaults():
    copy = MySQLCopy("foo", "daily", "some_file.txt", type="full")
    assert copy.backup_started is None
    assert copy.backup_finished is None
    assert copy.binlog is None
    assert copy.config == {}
    assert copy.host == "foo"
    assert copy.lsn is None
    assert copy.name == "some_file.txt"
    assert copy.parent is None
    assert copy.position is None
    assert copy.run_type == "daily"
    assert copy.galera is False
    assert copy.wsrep_provider_version is None
    assert copy.server_vendor == MySQLFlavor.ORACLE


def test_init_set_galera():
    copy = MySQLCopy(
        "foo",
        "daily",
        "some_file.txt",
        type="full",
        wsrep_provider_version="123",
    )
    assert copy.galera is True
    assert copy.wsrep_provider_version == "123"


@pytest.mark.parametrize(
    "value, vendor",
    [
        (
            "percona",
            MySQLFlavor.PERCONA,
        ),
        (
            MySQLFlavor.PERCONA,
            MySQLFlavor.PERCONA,
        ),
        (
            "percona",
            "percona",
        ),
    ],
)
def test_init_set_vendor(value, vendor):
    assert MySQLCopy("foo", "daily", "some_file.txt", type="full", server_vendor=value).server_vendor == vendor


def test_init_created_at():
    copy = MySQLCopy("foo", "daily", "some_file.txt", type="full", backup_started=123)
    assert copy.backup_started == 123
    assert copy.created_at == 123


@pytest.mark.parametrize(
    "path, host, run_type, name",
    [
        ("/foo/bar/hourly/mysql/mysql.tar.gz", "bar", "hourly", "mysql.tar.gz"),
        (
            "s3://twindb-www.twindb.com/master1/hourly/mysql/mysql-2016-11-23_21_50_54.xbstream.gz",
            "master1",
            "hourly",
            "mysql-2016-11-23_21_50_54.xbstream.gz",
        ),
        (
            "s3://twindb-www.twindb.com/master.box/hourly/mysql/mysql-2016-11-23_08_01_25.xbstream.gz",
            "master.box",
            "hourly",
            "mysql-2016-11-23_08_01_25.xbstream.gz",
        ),
        (
            "/path/to/twindb-server-backups/master1/daily/mysql/mysql-2016-11-23_21_47_21.xbstream.gz",
            "master1",
            "daily",
            "mysql-2016-11-23_21_47_21.xbstream.gz",
        ),
    ],
)
def test_copy_from_path(path, host, run_type, name):
    copy = MySQLCopy(path=path)
    assert copy.host == host
    assert copy.run_type == run_type
    assert copy.name == name
