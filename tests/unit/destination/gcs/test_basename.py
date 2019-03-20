from twindb_backup.destination.gcs import GCS, GCAuthOptions


def test_basename():
    dst = GCS('bucket', GCAuthOptions())
    assert dst.basename('gs://bucket/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'
