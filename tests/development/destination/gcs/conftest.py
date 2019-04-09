import pytest

from twindb_backup.destination.gcs import GCS


@pytest.fixture
def gs(creds_file):
    return GCS(
        gc_credentials_file=creds_file,
        bucket='twindb-backups'
    )


@pytest.fixture
def creds_file():
    return '/twindb_backup/env/My Project 17339-bbbc43d1bee3.json'
