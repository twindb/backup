from moto import mock_s3

from twindb_backup.destination.s3 import S3, AWSAuthOptions


@mock_s3
def test__create_bucket_creates_the_bucket():
    s3 = S3('test-bucket',
            AWSAuthOptions('access_key',
                           'secret_key')
            )
    s3.create_bucket()

    assert s3.s3_client.head_bucket(Bucket='test-bucket')
