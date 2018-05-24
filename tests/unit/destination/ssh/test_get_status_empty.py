# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh


# noinspection PyUnresolvedReferences
@mock.patch.object(Ssh, 'is_file_exist')
def test_get_status_empty(mock_status_exists):
    mock_status_exists.return_value = False

    dst = Ssh(remote_path='/foo/bar')
    status = dst.status()
    assert status.hourly == {}
    assert status.daily == {}
    assert status.weekly == {}
    assert status.monthly == {}
    assert status.yearly == {}
