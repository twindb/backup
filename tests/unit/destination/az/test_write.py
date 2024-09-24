import azure.core.exceptions as ae
import pytest

from .util import mocked_az

EXAMPLE_FILE = "test/backup.tar.gz"
CONTENT = b"test content"


def test_write_success():
    """Tests AZ.write method, ensuring the blob is written to azure."""
    c = mocked_az()

    c.write(CONTENT, EXAMPLE_FILE)

    c._container_client.upload_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), CONTENT, overwrite=True)


def test_write_fail():
    """Tests AZ.write method, re-raises an exception on failure"""
    c = mocked_az()
    c._container_client.upload_blob.side_effect = ae.HttpResponseError()

    with pytest.raises(Exception):
        c.write(CONTENT, EXAMPLE_FILE)

    c._container_client.upload_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), CONTENT, overwrite=True)
