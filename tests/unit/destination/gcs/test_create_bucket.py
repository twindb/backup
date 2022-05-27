import mock
import pytest
from google.cloud.exceptions import Conflict

from twindb_backup.destination.exceptions import GCSDestinationError
from twindb_backup.destination.gcs import GCS


@mock.patch.object(GCS, "_gcs_client")
def test_create_bucket_raises(mock_client):

    mock_client.create_bucket.side_effect = Conflict("error")

    gs = GCS(bucket="foo", gc_credentials_file="foo/bar")
    with pytest.raises(GCSDestinationError):
        gs.create_bucket()


@mock.patch.object(GCS, "_gcs_client")
def test_create_bucket(mock_client):

    gs = GCS(bucket="foo", gc_credentials_file="foo/bar")
    gs.create_bucket()
    mock_client.create_bucket.assert_called_once_with(bucket_name="foo")
