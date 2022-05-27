from subprocess import PIPE

import mock

from twindb_backup.modifiers import Lbzip2


@mock.patch("twindb_backup.modifiers.base.Popen")
def test_get_stream(mock_popen):
    mock_stream = mock.Mock()

    def foo(*args, **kwargs):
        pass

    mock_stream.__enter__ = foo
    mock_stream.__exit__ = foo

    m = Lbzip2(mock_stream, level=3, threads=123)
    with m.get_stream():
        mock_popen.assert_called_once_with(
            ["lbzip2", "-3", "-n", "123", "-c", "-"],
            stdin=None,
            stdout=PIPE,
            stderr=PIPE,
        )
