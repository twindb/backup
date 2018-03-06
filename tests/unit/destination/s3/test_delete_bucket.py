import pytest
from botocore.exceptions import ClientError
from moto import mock_s3

from twindb_backup.destination.s3 import S3, AWSAuthOptions


@mock_s3
def test__delete_bucket_deletes_the_bucket():
    s3 = S3('test-bucket',
            AWSAuthOptions('access_key',
                           'secret_key')
            )
    s3.create_bucket()

    s3.delete_bucket()

    with pytest.raises(ClientError):
        s3.s3_client.head_bucket(Bucket='test-bucket')
