from os import environ

import mock
import pytest

from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import GCSDestinationError
from twindb_backup.destination.gcs import GCS


def test_init_set_env():
    # make sure GOOGLE_APPLICATION_CREDENTIALS is not set
    if 'GOOGLE_APPLICATION_CREDENTIALS' in environ:
        del environ['GOOGLE_APPLICATION_CREDENTIALS']
    with pytest.raises(KeyError):
        assert environ['GOOGLE_APPLICATION_CREDENTIALS']

    GCS(bucket='bar', gc_credentials_file='foo')
    assert environ['GOOGLE_APPLICATION_CREDENTIALS'] == 'foo'


def test_init_set_bucket():
    gs = GCS(
        gc_credentials_file='foo',
        bucket='bar'
    )
    assert gs.bucket == 'bar'


@mock.patch.object(BaseDestination, '__init__')
def test_init_set_bucket(mock_base):
    mock_base.return_value = None
    gs = GCS(
        gc_credentials_file='foo',
        bucket='bar'
    )
    assert gs.bucket == 'bar'
    mock_base.assert_called_once_with(
        'bar'
    )


def test_init_raises_if_no_file():
    with pytest.raises(GCSDestinationError):
        GCS(bucket='foo')
