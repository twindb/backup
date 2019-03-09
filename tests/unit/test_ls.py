import mock
from twindb_backup.ls import list_available_backups


def test_list_available_backups_ssh():

    mock_config = mock.Mock()
    mock_config.keep_local_path = None

    # Ssh destination
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'
    mock_dst.list_files.return_value = []

    mock_config.destination.return_value = mock_dst

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
