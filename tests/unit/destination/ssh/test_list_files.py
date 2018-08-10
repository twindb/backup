import mock

from twindb_backup.destination.ssh import Ssh


def test_list_files():
    dst = Ssh(
        '/var/backups'
    )
    mock_client = mock.Mock()
    mock_client.list_files.return_value = [
        'foo',
        'bar'
    ]
    dst._ssh_client = mock_client
    assert dst.list_files('xxx', pattern='foo') == ['foo']
