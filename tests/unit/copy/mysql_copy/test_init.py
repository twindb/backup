import pytest

from twindb_backup.copy.exceptions import WrongInputData
from twindb_backup.copy.mysql_copy import MySQLCopy


def test_init_raises_if_name_is_not_relative():
    with pytest.raises(WrongInputData):
        MySQLCopy('foo', 'daily', 'some/non/relative/path', type='full')


def test_init_has_config():
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    assert copy.config == {}


def test_init_set_defaults():
    copy = MySQLCopy('foo', 'daily', 'some_file.txt', type='full')
    assert copy.backup_started is None
    assert copy.backup_finished is None
    assert copy.binlog is None
    assert copy.config == {}
    assert copy.host == 'foo'
    assert copy.lsn is None
    assert copy.name == 'some_file.txt'
    assert copy.parent is None
    assert copy.position is None
    assert copy.run_type == 'daily'
    assert copy.galera is False
    assert copy.wsrep_provider_version is None


def test_init_set_galera():
    copy = MySQLCopy(
        'foo', 'daily', 'some_file.txt',
        type='full',
        wsrep_provider_version='123'
    )
    assert copy.galera is True
    assert copy.wsrep_provider_version == '123'


def test_init_created_at():
    copy = MySQLCopy(
        'foo', 'daily', 'some_file.txt', type='full',
        backup_started=123)
    assert copy.backup_started == 123
    assert copy.created_at == 123
