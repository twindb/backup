import mock
from twindb_backup.destination.s3 import S3


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
    dst = S3('bucketname', 'b', 'c')
    assert dst.basename('s3://bucketname/some_dir/some_file.txt') == \
        'some_dir/some_file.txt'


def test_find_files():
    dst = S3('bucketname', 'b', 'c')
