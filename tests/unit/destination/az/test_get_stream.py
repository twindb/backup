from multiprocessing import Process
from unittest.mock import MagicMock, patch

import mock
import pytest

import twindb_backup.destination.az as az

from .util import mocked_az


def test_get_stream_success():
    """Tests AZ.get_stream method, mocks calls for process and os"""
    with patch("twindb_backup.destination.az.os") as mc_os:
        with patch("twindb_backup.destination.az.Process") as mc_process:
            mc = MagicMock(spec=Process)
            mc_process.return_value = mc
            mc.exitcode = 0

            mc_os.pipe.return_value = (100, 200)
            c = mocked_az()

            mock_copy = mock.Mock()
            mock_copy.key = "foo-key"

            with c.get_stream(mock_copy):
                pass

            az.Process.assert_called_once_with(target=c._download_to_pipe, args=(c.render_path("foo-key"), 100, 200))
            mc_os.close.assert_called_once_with(200)
            mc_os.fdopen.assert_called_once_with(100, "rb")
            mc.start.assert_called_once()
            mc.join.assert_called_once()


def test_get_stream_failure():
    """Tests AZ.get_stream method, raises an exception when child process fails"""
    with patch("twindb_backup.destination.az.os") as mc_os:
        with patch("twindb_backup.destination.az.Process") as mc_process:
            mc = MagicMock(spec=Process)
            mc_process.return_value = mc
            mc.exitcode = 1

            mc_os.pipe.return_value = (100, 200)
            c = mocked_az()

            mock_copy = mock.Mock()
            mock_copy.key = "foo-key"

            with pytest.raises(Exception):
                with c.get_stream(mock_copy):
                    pass

            az.Process.assert_called_once_with(target=c._download_to_pipe, args=(c.render_path("foo-key"), 100, 200))
            mc_os.close.assert_called_once_with(200)
            mc_os.fdopen.assert_called_once_with(100, "rb")
            mc.start.assert_called_once()
            mc.join.assert_called_once()
