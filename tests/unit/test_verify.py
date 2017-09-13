import mock

from twindb_backup.verify import get_latest_backup


def test_latest_backup():
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'
    mock_dst.status.return_value = {'daily': {'11': '111'}, 'hourly': {'22': '222'}}
    url = get_latest_backup(mock_dst)
    assert url == "/foo/bar/22"
