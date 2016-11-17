from ConfigParser import ConfigParser
import mock
import pytest
from twindb_backup.source.mysql_source import MySQLSource


@mock.patch.object(MySQLSource, 'get_prefix')
@mock.patch.object(MySQLSource, '_delete_local_files')
@mock.patch('twindb_backup.source.mysql_source.get_files_to_delete')
def test_apply_retention_policy(mock_get_files_to_delete,
                                mock_delete_local_files,
                                mock_get_prefix,
                                tmpdir):
    mock_get_files_to_delete.return_value = []
    mock_get_prefix.return_value = 'hourly/master.box'
    my_cnf = tmpdir.join('my.cnf')
    mock_config = mock.Mock()
    src = MySQLSource(str(my_cnf), 'hourly', mock.Mock(), mock.Mock())
    mock_dst = mock.Mock()
    mock_dst.remote_path = ''

    src.apply_retention_policy(mock_dst, mock_config, 'hourly')

    mock_delete_local_files.assert_called_once_with('mysql', mock_config)
    mock_dst.list_files.assert_called_once_with('hourly/master.box/mysql/mysql-')


@pytest.mark.parametrize('config_content,run_type,backup_type', [
    (
        """
[mysql]
full_backup=daily
        """,
        'yearly',
        'full'
    ),
    (
        """
[mysql]
full_backup=daily
        """,
        'hourly',
        'incremental'
    ),
    (
        """
[mysql]
full_backup=daily
        """,
        'daily',
        'full'
    ),
    (
        """
[mysql]
        """,
        'daily',
        'full'
    ),
    (
        """
[mysql]
full_backup=aaa
        """,
        'daily',
        'full'
    )
])
def test_get_backup_type(config_content,run_type,backup_type, tmpdir):
    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_content)
    cparser = ConfigParser()
    cparser.read(str(config_file))
    src = MySQLSource('/foo/bar', run_type, cparser, mock.Mock())
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
