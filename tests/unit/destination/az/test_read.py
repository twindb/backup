from unittest.mock import MagicMock

import azure.core.exceptions as ae
import pytest
from azure.storage.blob import StorageStreamDownloader

from twindb_backup.destination.exceptions import FileNotFound

from .util import mocked_az

EXAMPLE_FILE = "test/backup.tar.gz"


def test_read_success():
    """Tests AZ.read method, ensuring the blob is read from azure."""
    c = mocked_az()
    mock = MagicMock(StorageStreamDownloader)
    c._container_client.download_blob.return_value = mock

    c.read(EXAMPLE_FILE)

    c._container_client.download_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), encoding="utf-8")
    mock.read.assert_called_once()


def test_read_fail():
    """Tests AZ.read method, re-raises an exception on failure"""
    c = mocked_az()
    c._container_client.download_blob.side_effect = ae.HttpResponseError()

    with pytest.raises(Exception):
        c.read(EXAMPLE_FILE)
    c._container_client.download_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), encoding="utf-8")


def test_read_fail_not_found():
    """Tests AZ.read method, raising a twindb_backup.destination.exceptions.FileNotFound exception on ResourceNotFoundError"""
    c = mocked_az()
    c._container_client.download_blob.side_effect = ae.ResourceNotFoundError()

    with pytest.raises(
        FileNotFound, match=f"File {c.render_path(EXAMPLE_FILE)} does not exist in container {c._container_name}"
    ):
        c.read(EXAMPLE_FILE)
    c._container_client.download_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), encoding="utf-8")
