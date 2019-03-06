import mock
from twindb_backup.share import share


@mock.patch('twindb_backup.share.print')
# @mock.patch('twindb_backup.share.get_destination')
def test_share_backup_cli(mock_print):
    mock_config = mock.Mock()
    mock_config.get.return_value = "/foo/bar"
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'
    mock_config.destination.return_value = mock_dst
    mock_dst.find_files.return_value = ["/foo/bar1", "/foo/bar"]

    share(mock_config, "/foo/bar")

    mock_print.assert_called_once()
    mock_dst.share.assert_called_once_with("/foo/bar")
    mock_config.destination.assert_called_once_with()

