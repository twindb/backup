import mock

from twindb_backup.verify import get_latest_backup


def test_latest_backup():
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'
    mock_dst.status.return_value = {'daily': {'11': '111'}, 'hourly': {'22': '222'}}
    url = get_latest_backup(mock_dst)
    assert url == "/foo/bar/22"

def test_latest_backup_daily_is_newer():
    mock_dst = mock.Mock()
    mock_dst.remote_path = '/foo/bar'
    mock_dst.status.return_value = {'daily': {'22': '222'}, 'hourly': {'11': '111'}}
    url = get_latest_backup(mock_dst)
    assert url == "/foo/bar/22"
