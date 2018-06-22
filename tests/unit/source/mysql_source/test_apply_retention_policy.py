import mock

from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


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
    mock_dst.list_files.assert_called_once_with('/foo/bar/master.box/hourly/mysql/mysql-')
