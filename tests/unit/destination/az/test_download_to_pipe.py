from unittest.mock import MagicMock, patch

import azure.core.exceptions as ae
import pytest

from .util import mocked_az


def test_download_to_pipe_success():
    """Tests AZ.download_to_pipe method, mocks calls for os and ContainerClient"""
    with patch("twindb_backup.destination.az.os") as mc_os:
        mc_fdopen = MagicMock()
        mc_os.fdopen.return_value = mc_fdopen

        c = mocked_az()

        mc_dbr = MagicMock()
        c._container_client.download_blob.return_value = mc_dbr

        c._download_to_pipe(c.render_path("foo-key"), 100, 200)

        mc_os.close.assert_called_once_with(100)
        mc_os.fdopen.assert_called_once_with(200, "wb")
        c._container_client.download_blob.assert_called_once_with(c.render_path("foo-key"))
        mc_dbr.readinto.assert_called_once_with(mc_fdopen.__enter__())


def test_download_to_pipe_fail():
    """Tests AZ.download_to_pipe method, re-raises exception when download fails in child process"""
    with patch("twindb_backup.destination.az.os") as mc_os:
        c = mocked_az()

        c._container_client.download_blob.side_effect = ae.HttpResponseError()

        with pytest.raises(Exception):
            c._download_to_pipe(c.render_path("foo-key"), 100, 200)

        mc_os.close.assert_called_once_with(100)
        mc_os.fdopen.assert_called_once_with(200, "wb")
        c._container_client.download_blob.assert_called_once_with(c.render_path("foo-key"))
