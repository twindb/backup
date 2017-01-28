import mock
import pytest

from botocore.exceptions import ClientError
from moto import mock_s3
from twindb_backup.destination.s3 import S3


@mock_s3
def test__create_bucket_creates_the_bucket():
    s3 = S3('test-bucket', 'access_key', 'secret_key')
    s3.create_bucket()

    assert s3.s3_client.head_bucket(Bucket='test-bucket')


@mock_s3
def test__delete_bucket_deletes_the_bucket():
    s3 = S3('test-bucket', 'access_key', 'secret_key')
    s3.create_bucket()

    s3.delete_bucket()

    with pytest.raises(ClientError):
        s3.s3_client.head_bucket(Bucket='test-bucket')


@mock.patch.object(S3, '_status_exists')
@mock.patch.object(S3, 'setup_s3_client')
def test_get_status_empty(mock_status_exists, mock_setup_s3_client):
    mock_status_exists.return_value = False
    mock_setup_s3_client.return_value = None

    dst = S3('a', 'b', 'c')
    assert dst.status() == {
        'hourly': {},
        'daily': {},
        'weekly': {},
        'monthly': {},
        'yearly': {}
    }


@mock.patch.object(S3, 'setup_s3_client')
def test_basename(mock_setup_s3_client):
    mock_setup_s3_client.return_value = None

    dst = S3('bucket', 'b', 'c')
    assert dst.basename('s3://bucket/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'


@mock.patch.object(S3, 'setup_s3_client')
def test_find_files(mock_setup_s3_client):
    mock_setup_s3_client.return_value = None

    dst = S3('bucket', 'b', 'c')
