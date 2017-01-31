import boto3
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


@mock_s3
def test__list_files_returns_sorted_list():
    s3 = S3('test-bucket', 'access_key', 'secret_key')
    s3.create_bucket()

    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='object_1')

    files_list = s3.list_files(prefix='')
    assert len(files_list) == 1


@mock_s3
def test__find_files_returns_sorted_list_of_files():
    s3 = S3('test-bucket', 'access_key', 'secret_key')
    s3.create_bucket()

    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/file1.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/file2.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/daily/file1.txt')

    files_list = s3.find_files(prefix='', run_type='hourly')
    assert len(files_list) == 2
    assert files_list[0] == 's3://test-bucket/test_server/hourly/file1.txt'


@mock_s3
def test__delete_can_delete_an_object():
    twindb_s3 = S3('test-bucket', 'access_key', 'secret_key')
    twindb_s3.create_bucket()

    twindb_s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                                   Key='test_server/hourly/file1.txt')

    s3 = boto3.resource('s3')
    obj = s3.Object('test-bucket', 'test_server/hourly/file1.txt')

    assert twindb_s3.delete(obj)
    with pytest.raises(ClientError):
        twindb_s3.s3_client.head_object(Bucket='test-bucket',
                                        Key='test_server/hourly/file1.txt')


@mock.patch.object(S3, '_status_exists')
def test_get_status_empty(mock_status_exists):
    mock_status_exists.return_value = False

    dst = S3('a', 'b', 'c')
    assert dst.status() == {
        'hourly': {},
        'daily': {},
        'weekly': {},
        'monthly': {},
        'yearly': {}
    }


def test_basename():
    dst = S3('bucket', 'b', 'c')
    assert dst.basename('s3://bucket/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'
