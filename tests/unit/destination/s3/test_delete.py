import pytest
from botocore.exceptions import ClientError
from moto import mock_s3

from twindb_backup.destination.s3 import AWSAuthOptions, S3


@mock_s3
def test__delete_can_delete_an_object():
    twindb_s3 = S3('test-bucket',
                   AWSAuthOptions('access_key',
                                  'secret_key')
                   )
    twindb_s3.create_bucket()

    twindb_s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                                   Key='test_server/hourly/file1.txt')

    assert twindb_s3.delete('test_server/hourly/file1.txt')
    with pytest.raises(ClientError):
        twindb_s3.s3_client.head_object(Bucket='test-bucket',
                                        Key='test_server/hourly/file1.txt')
