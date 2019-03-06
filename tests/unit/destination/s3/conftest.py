import pytest
from twindb_backup.destination.s3 import S3


@pytest.fixture
def s3():
    return S3(
        bucket='test-bucket',
        aws_access_key_id='access_key',
        aws_secret_access_key='secret_key'
    )
