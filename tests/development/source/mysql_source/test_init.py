import mock
import pytest

from twindb_backup.source.exceptions import MySQLSourceError
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


def test_mysql_source_has_methods():
    src = MySQLSource(
        MySQLConnectInfo('/foo/bar'),
        'hourly', 'full',
        dst=mock.Mock()
    )
    assert src._connect_info.defaults_file == '/foo/bar'
    assert src.run_type == 'hourly'
    assert src.suffix == 'xbstream'
    assert src._media_type == 'mysql'


def test_mysql_source_raises_on_wrong_connect_info():
    with pytest.raises(MySQLSourceError):
        MySQLSource(
            '/foo/bar',
            'hourly', 'full',
            dst=mock.Mock()
        )


@pytest.mark.parametrize('run_type', [
    'foo',
    None,
    ''
])
def test_mysql_raises_on_wrong_run_type(run_type):
    with pytest.raises(MySQLSourceError):
        MySQLSource(
            MySQLConnectInfo('/foo/bar'),
            'foo', 'full',
            dst=mock.Mock()
        )
