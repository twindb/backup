import mock
import pytest
from pymysql import InternalError

from twindb_backup.backup import binlogs_to_backup
from twindb_backup.exceptions import OperationError


@pytest.mark.parametrize("binlog_names, last_binlog, to_backup", [
    (
        [
            {
                'Log_name': 'mysql-bin.000001'
            },
            {
                'Log_name': 'mysql-bin.000002'
            },
            {
                'Log_name': 'mysql-bin.000003'
            },

        ],
        'mysql-bin.000002',
        []
    ),
    (
        [
            {
                'Log_name': 'mysql-bin.000001'
            },
            {
                'Log_name': 'mysql-bin.000002'
            },
            {
                'Log_name': 'mysql-bin.000003'
            },

        ],
        'mysql-bin.000001',
        [
            'mysql-bin.000002'
        ]
    ),
    (
        [
            {
                'Log_name': 'mysql-bin.000001'
            },
            {
                'Log_name': 'mysql-bin.000002'
            },
            {
                'Log_name': 'mysql-bin.000003'
            },

        ],
        None,
        [
            'mysql-bin.000001',
            'mysql-bin.000002'
        ]
    ),
    (
        [],
        None,
        []
    ),
])
def test_binlogs_to_backup(binlog_names, last_binlog, to_backup):
    mock_cursor = mock.Mock()
    mock_cursor.fetchall.return_value = binlog_names
    assert to_backup == binlogs_to_backup(
        mock_cursor,
        last_binlog=last_binlog
    )


def test_binlog_returns_empty():
    mock_cursor = mock.Mock()
    mock_cursor.execute.side_effect = InternalError(
        1381, u'You are not using binary logging'
    )
    assert binlogs_to_backup(mock_cursor) == []


@pytest.mark.parametrize('side_effect', [
    InternalError,
    InternalError(123, 'Some other error')
])
def test_binlog_raises(side_effect):
    mock_cursor = mock.Mock()
    mock_cursor.execute.side_effect = side_effect
    with pytest.raises(OperationError):
        binlogs_to_backup(mock_cursor)
