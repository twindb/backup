from moto import mock_s3

from twindb_backup.destination.s3 import AWSAuthOptions, S3


@mock_s3
def test__get_files_returns_sorted_list_of_files():
    s3 = S3('test-bucket',
            AWSAuthOptions('access_key',
                           'secret_key')
            )
    s3.create_bucket()

    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/file1.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/file2.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/daily/file1.txt')

    files_list = s3.get_files(prefix='', interval='hourly')
    assert len(files_list) == 2
    assert files_list[0] == 's3://test-bucket/test_server/hourly/file1.txt'


@mock_s3
def test_get_files_using_run_type_and_copy_type():
    s3 = S3('test-bucket',
            AWSAuthOptions('access_key',
                           'secret_key')
            )
    s3.create_bucket()
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/files/file1.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/files/file2.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/daily/files/file3.txt')

    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/weekly/mysql/file1.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/mysql/file2.txt')
    s3.s3_client.put_object(Body='hello world', Bucket='test-bucket',
                            Key='test_server/hourly/mysql/file3.txt')
    files_list = s3.get_files(prefix='', interval='hourly', copy_type="files")
    assert len(files_list) == 2
    files_list = s3.get_files(prefix='', interval='weekly', copy_type="mysql")
    assert len(files_list) == 1

    files_list = s3.get_files(prefix='', copy_type="files")
    assert len(files_list) == 3
