from subprocess import PIPE

import mock

from twindb_backup.modifiers import Gzip


@mock.patch("twindb_backup.modifiers.base.Popen")
def test_get_stream(mock_popen):
    mock_stream = mock.Mock()

    def foo(*args, **kwargs):
        pass

    mock_stream.__enter__ = foo
    mock_stream.__exit__ = foo

    m = Gzip(mock_stream, level=3)
    with m.get_stream():
        mock_popen.assert_called_once_with(["gzip", "-3", "-c", "-"], stdin=None, stdout=PIPE, stderr=PIPE)
