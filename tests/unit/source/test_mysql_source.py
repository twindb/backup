import logging
import mock
import pytest

from pymysql.err import InternalError
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo, \
    MySQLSourceError


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


@mock.patch('twindb_backup.source.base_source.socket')
@mock.patch('twindb_backup.source.base_source.time')
def test_get_name(mock_time, mock_socket):

    host = 'some-host'
    mock_socket.gethostname.return_value = host
    timestamp = '2017-02-13_15_40_29'
    mock_time.strftime.return_value = timestamp

    src = MySQLSource(
        MySQLConnectInfo('/foo/bar'),
        'daily', 'full',
        dst=mock.Mock()
    )

    assert src.get_name() == "some-host/daily/mysql/mysql-2017-02-13_15_40_29.xbstream"


@mock.patch.object(MySQLSource, 'get_prefix')
@mock.patch.object(MySQLSource, '_delete_local_files')
@mock.patch('twindb_backup.source.mysql_source.get_files_to_delete')
def test_apply_retention_policy(mock_get_files_to_delete,
                                mock_delete_local_files,
                                mock_get_prefix,
                                tmpdir):
    mock_get_files_to_delete.return_value = []
    mock_get_prefix.return_value = 'master.box/hourly'
    my_cnf = tmpdir.join('my.cnf')
    mock_config = mock.Mock()
    src = MySQLSource(
        MySQLConnectInfo(str(my_cnf)),
        'hourly',
        'full',
        dst=mock.Mock()
    )
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'

    src.apply_retention_policy(mock_dst, mock_config, 'hourly', mock.Mock())

    mock_delete_local_files.assert_called_once_with('mysql', mock_config)
    mock_dst.get_files.assert_called_once_with('/foo/bar/master.box/hourly/mysql/mysql-')


@mock.patch.object(MySQLSource, 'get_connection')
def test__enable_wsrep_desync_sets_wsrep_desync_to_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    source.enable_wsrep_desync()

    mock_cursor.execute.assert_called_with("SET GLOBAL wsrep_desync=ON")


@mock.patch.object(MySQLSource, 'get_connection')
def test__disable_wsrep_desync_sets_wsrep_desync_to_off(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchall.return_value = [
        {'Variable_name': 'wsrep_local_recv_queue', 'Value': '0'},
    ]

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    source.disable_wsrep_desync()

    mock_cursor.execute.assert_any_call("SHOW GLOBAL STATUS LIKE "
                                        "'wsrep_local_recv_queue'")
    mock_cursor.execute.assert_called_with("SET GLOBAL wsrep_desync=OFF")


@mock.patch.object(MySQLSource, 'get_connection')
def test__is_galera_returns_false_on_int_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {'wsrep_on': 0}

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    assert source.is_galera() is False


@mock.patch.object(MySQLSource, 'get_connection')
def test__is_galera_returns_true_on_int_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {'wsrep_on': 1}

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    assert source.is_galera() is True


@mock.patch.object(MySQLSource, 'get_connection')
def test__is_galera_returns_true_on_str_higher_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {'wsrep_on': 'ON'}

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    assert source.is_galera() is True


@mock.patch.object(MySQLSource, 'get_connection')
def test__is_galera_returns_false_on_str_higher_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {'wsrep_on': 'OFF'}

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    assert source.is_galera() is False


@mock.patch.object(MySQLSource, 'get_connection')
def test__is_galera_returns_true_on_galera_node(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = InternalError(1193,
                                                    "Unknown system variable "
                                                    "'wsrep_on'")

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    assert source.is_galera() is False


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


def test__get_connection_raises_mysql_source_error():
    source = MySQLSource(MySQLConnectInfo(None), 'daily', 'full')
    with pytest.raises(MySQLSourceError):
        with source.get_connection():
            pass
