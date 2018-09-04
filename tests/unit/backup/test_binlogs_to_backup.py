import mock
import pytest

from twindb_backup.backup import binlogs_to_backup


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
        [
            '/foo/bar/mysql-bin.000003'
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
            '/foo/bar/mysql-bin.000001',
            '/foo/bar/mysql-bin.000002',
            '/foo/bar/mysql-bin.000003',
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
        '/foo/bar',
        last_binlog=last_binlog
    )
