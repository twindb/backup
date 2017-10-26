# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh


# noinspection PyUnresolvedReferences
@mock.patch.object(Ssh, '_status_exists')
def test_get_status_empty(mock_status_exists):
    mock_status_exists.return_value = False

    dst = Ssh(remote_path='/foo/bar')
    assert dst.status() == {
        'hourly': {},
        'daily': {},
        'weekly': {},
        'monthly': {},
        'yearly': {}
    }
