# noinspection PyPackageRequirements
import mock

from twindb_backup.destination.ssh import Ssh


# noinspection PyUnresolvedReferences
@mock.patch.object(Ssh, '_mkdir_r')
def test_mkdirname_r(mock_mkdir_r):
    dst = Ssh(remote_path='')
    # noinspection PyProtectedMember
    dst._mkdirname_r('/foo/bar/xyz')
    mock_mkdir_r.assert_called_once_with('/foo/bar')
