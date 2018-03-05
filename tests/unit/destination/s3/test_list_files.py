from moto import mock_s3

from twindb_backup.destination.s3 import S3, AWSAuthOptions


@mock_s3
def test__list_files_returns_sorted_list():
    s3 = S3('test-bucket',
            AWSAuthOptions('access_key',
                           'secret_key')
            )
    s3.create_bucket()

    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='object_1')

    files_list = s3.list_files(prefix='')
    assert len(files_list) == 1
