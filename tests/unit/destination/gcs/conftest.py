import pytest

from twindb_backup.destination.gcs import GCS


@pytest.fixture
def gs():
    return GCS(
        bucket='test-bucket',
        gc_credentials_file='foo'
    )
