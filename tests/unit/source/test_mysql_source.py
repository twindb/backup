from ConfigParser import ConfigParser
import logging
import mock
import pytest

from pymysql.err import InternalError
from twindb_backup.source.mysql_source import MySQLSource


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
    src = MySQLSource(str(my_cnf), 'hourly', mock.Mock(), mock.Mock())
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'

    src.apply_retention_policy(mock_dst, mock_config, 'hourly', mock.Mock())

    mock_delete_local_files.assert_called_once_with('mysql', mock_config)
    mock_dst.list_files.assert_called_once_with('/foo/bar/master.box/hourly/mysql/mysql-')


@pytest.mark.parametrize('config_content,run_type,backup_type,status', [
    (
        """
[mysql]
full_backup=daily
        """,
        'yearly',
        'full',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }
    ),
    (
        """
[mysql]
full_backup=daily
        """,
        'hourly',
        'full',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }
    ),
    (
        """
[mysql]
full_backup=daily
        """,
        'hourly',
        'incremental',
        {
            'hourly': {},
            'daily': {'foo': {}},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }
    ),
    (
        """
[mysql]
full_backup=daily
        """,
        'daily',
        'full',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }
    ),
    (
        """
[mysql]
        """,
        'daily',
        'full',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }
    ),
    (
        """
[mysql]
full_backup=aaa
        """,
        'daily',
        'full',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }
    ),
    (
        """
[mysql]
full_backup=weekly
        """,
        'hourly',
        'full',
        {
            "daily": {
                "master.box/daily/mysql/mysql-2016-11-20_04_13_10.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19630043,
                    "position": 44726,
                    "type": "full"
                }
            },
            "hourly": {
                "master.box/hourly/mysql/mysql-2016-11-20_04_12_26.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19630035,
                    "position": 44726,
                    "type": "full"
                },
                "master.box/hourly/mysql/mysql-2016-11-20_04_13_36.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19630051,
                    "parent": "master.box/daily/mysql/mysql-2016-11-20_04_13_10.xbstream.gz",
                    "position": 44726,
                    "type": "incremental"
                }
            },
            "monthly": {},
            "weekly": {},
            "yearly": {}
        }
    )
])
def test_get_backup_type(config_content, run_type, backup_type, status, tmpdir):
    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_content)
    cparser = ConfigParser()
    cparser.read(str(config_file))
    mock_dst = mock.Mock()
    mock_dst.status.return_value = status
    src = MySQLSource('/foo/bar', run_type, cparser, mock_dst)
    assert src._get_backup_type() == backup_type


@pytest.mark.parametrize('config_content,run_type,status', [
    (
        """
[mysql]
full_backup=daily
        """,
        'hourly',
        {
            "daily": {
                "master.box/daily/mysql/mysql-2016-11-17_04_55_40.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19629396,
                    "position": 43670
                },
                "master.box/daily/mysql/mysql-2016-11-17_04_56_28.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19629404,
                    "position": 43670
                },
                "master.box/daily/mysql/mysql-2016-11-17_05_05_33.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19629412,
                    "position": 43670
                }
            },
            "hourly": {},
            "monthly": {},
            "weekly": {},
            "yearly": {}
        }
    )
])
def test_last_full_lsn(config_content, run_type, status, tmpdir):

    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_content)
    cparser = ConfigParser()
    cparser.read(str(config_file))
    mock_dst = mock.Mock()
    mock_dst.status.return_value = status
    src = MySQLSource('/foo/bar', run_type, cparser, mock_dst)
    assert src.parent_lsn == 19629412


@pytest.mark.parametrize('run_type, full_backup, status, expected', [
    (
        'daily',
        'weekly',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        },
        False
    ),
    (
        'daily',
        'weekly',
        {
            'hourly': {'foo': {}},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        },
        False
    ),
    (
        'daily',
        'weekly',
        {
            'hourly': {},
            'daily': {},
            'weekly': {'foo': {}},
            'monthly': {},
            'yearly': {}
        },
        True
    ),
    (
        'hourly',
        'daily',
        {
            'hourly': {},
            'daily': {'foo': {}},
            'weekly': {'foo': {}},
            'monthly': {},
            'yearly': {}
        },
        True
    ),
    (
        'hourly',
        'daily',
        {
            'hourly': {},
            'daily': {},
            'weekly': {'foo': {}},
            'monthly': {},
            'yearly': {}
        },
        True
    ),
    (
        'hourly',
        'weekly',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        },
        False
    ),
    (
        'hourly',
        'weekly',
        {
            "daily": {
                "master.box/daily/mysql/mysql-2016-11-20_04_13_10.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19630043,
                    "position": 44726,
                    "type": "full"
                }
            },
            "hourly": {
                "master.box/hourly/mysql/mysql-2016-11-20_04_12_26.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19630035,
                    "position": 44726,
                    "type": "full"
                },
                "master.box/hourly/mysql/mysql-2016-11-20_04_13_36.xbstream.gz": {
                    "binlog": "mysql-bin.000001",
                    "lsn": 19630051,
                    "parent": "master.box/daily/mysql/mysql-2016-11-20_04_13_10.xbstream.gz",
                    "position": 44726,
                    "type": "incremental"
                }
            },
            "monthly": {},
            "weekly": {},
            "yearly": {}
        },
        False
    )

])
def test_parent_exists(run_type, full_backup, status, expected):

    mock_dst = mock.Mock()
    mock_dst.status.return_value = status
    mock_config = mock.Mock()
    mock_config.get.return_value = full_backup
    src = MySQLSource('/foo/bar', run_type, mock_config, mock_dst)
    assert src._parent_exists() == expected


@pytest.mark.parametrize('status, run_type, remote, fl, expected_status', [
    (
        {
            'hourly': {'master.box/hourly/mysql/mysql-2016-11-20_22_31_09.xbstream.gz': {}},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        },
        'hourly',
        '/path/to/twindb-server-backups',
        '/path/to/twindb-server-backups/master.box/hourly/mysql/mysql-2016-11-20_22_31_09.xbstream.gz',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }

    ),
    (
        {
            'hourly': {'master.box/hourly/mysql/mysql-2016-11-20_22_31_09.xbstream.gz': {}},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        },
        'hourly',
        '/path/to/twindb-server-backups/',
        '/path/to/twindb-server-backups/master.box/hourly/mysql/mysql-2016-11-20_22_31_09.xbstream.gz',
        {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }

    ),
    (
        {u'monthly': {},
         u'hourly': {'master.box/hourly/mysql/mysql-2016-11-21_00_05_12.xbstream.gz': {'lsn': 19746098, 'binlog': 'mysql-bin.000001', 'type': 'incremental', 'parent': u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz', 'position': 80706}, u'master.box/hourly/mysql/mysql-2016-11-20_23_55_05.xbstream.gz': {u'lsn': 19746074, u'binlog': u'mysql-bin.000001', u'type': u'incremental', u'parent': u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz', u'position': 80706}, u'master.box/hourly/mysql/mysql-2016-11-20_23_53_57.xbstream.gz': {u'lsn': 19746066, u'binlog': u'mysql-bin.000001', u'type': u'incremental', u'parent': u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz', u'position': 80706}}, u'daily': {u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz': {u'lsn': 19746058, u'binlog': u'mysql-bin.000001', u'type': u'full', u'position': 80706}},
         u'yearly': {},
         u'weekly': {}},
        'hourly',
        '/path/to/twindb-server-backups/',
        '/path/to/twindb-server-backups/master.box/hourly/mysql/mysql-2016-11-20_23_56_12.xbstream.gz',
        {u'monthly': {},
         u'hourly': {'master.box/hourly/mysql/mysql-2016-11-21_00_05_12.xbstream.gz': {'lsn': 19746098, 'binlog': 'mysql-bin.000001', 'type': 'incremental', 'parent': u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz', 'position': 80706}, u'master.box/hourly/mysql/mysql-2016-11-20_23_55_05.xbstream.gz': {u'lsn': 19746074, u'binlog': u'mysql-bin.000001', u'type': u'incremental', u'parent': u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz', u'position': 80706}, u'master.box/hourly/mysql/mysql-2016-11-20_23_53_57.xbstream.gz': {u'lsn': 19746066, u'binlog': u'mysql-bin.000001', u'type': u'incremental', u'parent': u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz', u'position': 80706}}, u'daily': {u'master.box/daily/mysql/mysql-2016-11-20_23_53_22.xbstream.gz': {u'lsn': 19746058, u'binlog': u'mysql-bin.000001', u'type': u'full', u'position': 80706}},
         u'yearly': {},
         u'weekly': {}}
    )
])
def test_delete_from_status(status, run_type, remote, fl, expected_status):
    src = MySQLSource('/foo/bar', run_type, mock.Mock(), mock.Mock())
    assert src._delete_from_status(status, remote, fl) == expected_status


@mock.patch('twindb_backup.source.mysql_source.get_connection')
def test__enable_wsrep_desync_sets_wsrep_desync_to_on(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(None, None, None, None)
    source.enable_wsrep_desync()

    mock_cursor.execute.assert_called_with("SET GLOBAL wsrep_desync=ON")


@mock.patch('twindb_backup.source.mysql_source.get_connection')
def test__disable_wsrep_desync_sets_wsrep_desync_to_off(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchall.return_value = [
        {'Variable_name': 'wsrep_local_recv_queue', 'Value': '0'},
    ]

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(None, None, None, None)
    source.disable_wsrep_desync()

    mock_cursor.execute.assert_any_call("SHOW GLOBAL STATUS LIKE "
                                        "'wsrep_local_recv_queue'")
    mock_cursor.execute.assert_called_with("SET GLOBAL wsrep_desync=OFF")


@mock.patch('twindb_backup.source.mysql_source.get_connection')
def test__is_galera_returns_true_on_galera_node(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = {'wsrep_on': 'ON'}

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(None, None, None, None)
    assert source.is_galera()


@mock.patch('twindb_backup.source.mysql_source.get_connection')
def test__is_galera_returns_true_on_galera_node(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = InternalError(1193,
                                                    "Unknown system variable "
                                                    "'wsrep_on'")

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(None, None, None, None)
    assert source.is_galera() is False


@mock.patch('twindb_backup.source.mysql_source.get_connection')
def test__wsrep_provider_version_returns_correct_version(mock_connect):
    logging.basicConfig()

    mock_cursor = mock.MagicMock()
    mock_cursor.fetchall.return_value = [
        {'Variable_name': 'wsrep_provider_version', 'Value': '3.19(rb98f92f)'},
    ]

    mock_connect.return_value.__enter__.return_value. \
        cursor.return_value.__enter__.return_value = mock_cursor

    source = MySQLSource(None, None, None, None)
    assert source.wsrep_provider_version == '3.19'
