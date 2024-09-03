import random
import string

import azure.core.exceptions as ae
import pytest
from azure.storage.blob import BlobProperties

from .util import mocked_az

PREFIX = "/backups/mysql"

BLOBS = [
    BlobProperties(name="blob1", metadata={"hdi_isfolder": "true"}),
    BlobProperties(name="blob2", metadata={"hdi_isfolder": "false"}),
    BlobProperties(name="blob3"),
]


def test_list_files_success():
    """Tests AZ.list_files method, reading a list of blob names from azure."""
    c = mocked_az()
    c._container_client.list_blobs.return_value = BLOBS

    blobs = c._list_files()
    assert blobs == [b.name for b in BLOBS]

    c._container_client.list_blobs.assert_called_once()


def test_list_files_fail():
    """Tests AZ.list_files method, re-raises an exception on failure"""
    c = mocked_az()
    c._container_client.list_blobs.side_effect = ae.HttpResponseError()

    with pytest.raises(Exception):
        c._list_files(PREFIX, False, False)

    c._container_client.list_blobs.assert_called_once_with(name_starts_with=PREFIX, include=["metadata"])


def test_list_files_files_only():
    """Tests AZ.list_files method, listing only file blobs"""
    c = mocked_az()
    c._container_client.list_blobs.return_value = BLOBS

    blob_names = c._list_files(PREFIX, False, True)

    assert blob_names == ["blob2", "blob3"]

    c._container_client.list_blobs.assert_called_once_with(name_starts_with=PREFIX, include=["metadata"])


def test_list_files_all_files():
    """Tests AZ.list_files method, listing all blobs, including directories"""
    c = mocked_az()
    c._container_client.list_blobs.return_value = BLOBS

    blob_names = c._list_files(PREFIX, False, False)

    assert blob_names == [b.name for b in BLOBS]

    c._container_client.list_blobs.assert_called_once_with(name_starts_with=PREFIX, include=["metadata"])


def test_list_files_recursive():
    """Tests AZ.list_files method, recursive option is ignored"""
    c = mocked_az()
    c._container_client.list_blobs.return_value = BLOBS

    blob_names = c._list_files(PREFIX, False, False)
    blob_names_recursive = c._list_files(PREFIX, True, False)

    assert blob_names == blob_names_recursive
    c._container_client.list_blobs.assert_called_with(name_starts_with=PREFIX, include=["metadata"])


def test_list_files_prefix():
    """Tests AZ.list_files method, prefix is used as a filter in list_blobs only"""
    c = mocked_az()
    c._container_client.list_blobs.return_value = BLOBS

    # Prefix is used as a filter in list_blobs, and because its mocked - it makes no difference.
    blob_names = c._list_files("".join(random.SystemRandom().choices(string.ascii_lowercase, k=10)), False, False)
    blob_names_recursive = c._list_files(PREFIX, False, False)

    assert blob_names == blob_names_recursive
