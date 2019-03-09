import pytest
from botocore.exceptions import ClientError
from moto import mock_s3


@pytest.mark.parametrize('path, key', [
    (
        'test_server/hourly/file1.txt',
        'test_server/hourly/file1.txt'
    ),
    (
        's3://test-bucket/test_server/hourly/file1.txt',
        'test_server/hourly/file1.txt'
    )
])
@mock_s3
def test__delete_can_delete_an_object(path, key, s3):
    twindb_s3 = s3
    twindb_s3.create_bucket()

    twindb_s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key=key
    )

    assert twindb_s3.delete(path)
    with pytest.raises(ClientError):
        twindb_s3.s3_client.head_object(
            Bucket='test-bucket',
            Key=key
        )
