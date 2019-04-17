import mock
import pytest
from google.cloud.exceptions import NotFound

from twindb_backup.destination.exceptions import GCSDestinationError
from twindb_backup.destination.gcs import GCS


@mock.patch.object(GCS, '_gcs_client')
def test_delete_bucket_raises(mock_client):

    mock_client.get_bucket.side_effect = NotFound('error')

    gs = GCS(
        bucket='foo',
        gc_credentials_file='foo/bar'
    )
    with pytest.raises(GCSDestinationError):
        gs.delete_bucket()


@pytest.mark.parametrize('force', [
    True, False
])
@mock.patch.object(GCS, '_gcs_client')
def test_delete_bucket(mock_client, force):

    mock_bucket = mock.Mock()
    mock_client.get_bucket.return_value = mock_bucket

    gs = GCS(
        bucket='foo',
        gc_credentials_file='foo/bar'
    )
    gs.delete_bucket(force=force)
    mock_bucket.delete.assert_called_once_with(
        force=force
    )
