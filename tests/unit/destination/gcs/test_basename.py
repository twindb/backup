from twindb_backup.destination.gcs import GCS


def test_basename():
    dst = GCS(bucket='bucket', gc_credentials_file=None)
    assert dst.basename('gs://bucket/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'
