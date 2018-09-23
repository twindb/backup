import mock
from twindb_backup.ls import list_available_backups


@mock.patch('twindb_backup.ls.get_destination')
def test_list_available_backups_ssh(mock_get_destination, tmpdir):

    backup_dir = tmpdir.mkdir('backups')

    mock_config = mock.Mock()
    mock_config.get.return_value = str(backup_dir)

    # Ssh destination
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'

    mock_get_destination.return_value = mock_dst
    mock_dst.list_files.return_value = []

    list_available_backups(mock_config)

    calls = [
        mock.call(
            '/foo/bar', pattern='/hourly/files/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/daily/files/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/weekly/files/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/monthly/files/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/yearly/files/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/hourly/mysql/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/daily/mysql/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/weekly/mysql/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/monthly/mysql/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', pattern='/yearly/mysql/', recursive=True, files_only=True
        ),
        mock.call(
            '/foo/bar', files_only=True, pattern='/binlog/', recursive=True
        )
    ]
    mock_dst.list_files.assert_has_calls(calls)
