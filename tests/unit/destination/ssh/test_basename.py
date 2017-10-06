from twindb_backup.destination.ssh import Ssh


def test_basename():
    dst = Ssh(remote_path='/foo/bar')
    assert dst.basename('/foo/bar/some_dir/some_file.txt') \
        == 'some_dir/some_file.txt'
