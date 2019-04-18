import mock
import pytest

from twindb_backup.destination.exceptions import FileNotFound
from twindb_backup.destination.gcs import GCS


@mock.patch.object(GCS, '_bucket_obj')
def test_delete_raises_not_found(mock_bucket):
    # Actually list_blobs() returns HTTPIterator. For test simplicity we'll
    # use empty iterator.
    mock_bucket.list_blobs.return_value = iter([])

    gs = GCS(bucket='bar', gc_credentials_file='xyz')

    with pytest.raises(FileNotFound):
        gs.delete('foo')

    mock_bucket.list_blobs.assert_called_once_with(
        prefix='foo'
    )


@mock.patch.object(GCS, '_bucket_obj')
def test_deletes_only_fully_matching(mock_bucket):
    mock_blob1 = mock.Mock()
    mock_blob1.name = 'foo'
    mock_blob2 = mock.Mock()
    mock_blob2.name = 'foobar'
    mock_bucket.list_blobs.return_value = iter(
        [
            mock_blob1, mock_blob2
        ]
    )

    gs = GCS(bucket='somebucket', gc_credentials_file='xyz')
    gs.delete('foo')

    mock_blob1.delete.assert_called_once_with()
    mock_blob2.delete.assert_not_called()


@mock.patch.object(GCS, '_bucket_obj')
def test_deletes_chunks(mock_bucket):
    mock_blob1 = mock.Mock()
    mock_blob1.name = 'foo/part-0000000000000000'
    mock_blob2 = mock.Mock()
    mock_blob2.name = 'foo/part-0000000000000001'
    mock_bucket.list_blobs.return_value = iter(
        [
            mock_blob1, mock_blob2
        ]
    )

    gs = GCS(bucket='somebucket', gc_credentials_file='xyz')
    gs.delete('foo')

    mock_blob1.delete.assert_called_once_with()
    mock_blob2.delete.assert_called_once_with()
