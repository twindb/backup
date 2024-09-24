from typing import BinaryIO
from unittest.mock import MagicMock

import azure.core.exceptions as ae
import pytest

from .util import mocked_az

EXAMPLE_FILE = "test/backup.tar.gz"


def test_save_success():
    """Tests AZ.save method, ensuring the blob is saved to azure."""
    c = mocked_az()
    handler = MagicMock(BinaryIO)
    file_obj = MagicMock()
    handler.__enter__.return_value = file_obj
    handler.__exit__.return_value = None

    c.save(handler, EXAMPLE_FILE)

    c._container_client.upload_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), file_obj)


def test_save_fail():
    """Tests AZ.save method, re-raises an exception on failure"""
    c = mocked_az()
    handler = MagicMock(BinaryIO)
    file_obj = MagicMock()
    handler.__enter__.return_value = file_obj
    handler.__exit__.return_value = None
    c._container_client.upload_blob.side_effect = ae.HttpResponseError()

    with pytest.raises(Exception):
        c.save(handler, EXAMPLE_FILE)

    c._container_client.upload_blob.assert_called_once_with(c.render_path(EXAMPLE_FILE), file_obj)
