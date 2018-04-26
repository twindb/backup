from twindb_backup.destination.s3 import S3, AWSAuthOptions


def test_basename():
    dst = S3('bucket',
             AWSAuthOptions('b',
                            'c'))
    assert dst.basename('s3://bucket/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'
