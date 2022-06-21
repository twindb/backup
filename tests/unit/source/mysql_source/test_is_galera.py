import logging

import mock
import pytest
from pymysql import InternalError, OperationalError

from twindb_backup.source.mysql_source import MySQLConnectInfo, MySQLSource


@mock.patch.object(MySQLSource, "get_connection")
def test__is_galera_returns_false_on_int_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {"wsrep_on": 0}

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    assert source.is_galera() is False


@mock.patch.object(MySQLSource, "get_connection")
def test__is_galera_returns_true_on_int_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {"wsrep_on": 1}

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    assert source.is_galera() is True


@mock.patch.object(MySQLSource, "get_connection")
def test__is_galera_returns_true_on_str_higher_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {"wsrep_on": "ON"}

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    assert source.is_galera() is True


@mock.patch.object(MySQLSource, "get_connection")
def test__is_galera_returns_false_on_str_higher_wsrep_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {"wsrep_on": "OFF"}

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    assert source.is_galera() is False


@pytest.mark.parametrize(
    "side_effect",
    [
        InternalError(1193, "Unknown system variable " "'wsrep_on'"),
        OperationalError(1193, "Unknown system variable " "'wsrep_on'"),
    ],
)
@mock.patch.object(MySQLSource, "get_connection")
def test__is_galera_returns_true_on_galera_node(mock_connect, side_effect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = side_effect

    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(MySQLConnectInfo(None), "daily", "full")
    assert source.is_galera() is False
