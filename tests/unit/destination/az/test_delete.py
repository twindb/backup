import pytest

import twindb_backup.destination.az as az

from .util import mocked_az


def test_delete_success():
    """Tests AZ.delete method, ensuring the blob is deleted."""
    c = mocked_az()

    c.delete("test")
    c._container_client.delete_blob.assert_called_once_with(c.render_path("test"))


def test_delete_fail():
    """Tests AZ.delete method, re-raising an exception on failure"""
    c = mocked_az()
    c._container_client.delete_blob.side_effect = Exception()

    with pytest.raises(Exception):
        c.delete("test")
    c._container_client.delete_blob.assert_called_once_with(c.render_path("test"))
