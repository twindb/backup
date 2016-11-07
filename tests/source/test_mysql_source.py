import mock
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
    src = MySQLSource(str(my_cnf), 'hourly')
    mock_dst = mock.Mock()
    mock_dst.remote_path = ''

    src.apply_retention_policy(mock_dst, mock_config, 'hourly')

    mock_delete_local_files.assert_called_once_with('mysql', mock_config)
    mock_dst.list_files.assert_called_once_with('hourly/master.box/mysql/mysql-')
