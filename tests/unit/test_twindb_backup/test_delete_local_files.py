import mock
import pytest

from twindb_backup import delete_local_files


@pytest.mark.parametrize(
    "keep, calls",
    [
        (1, [mock.call("aaa"), mock.call("bbb")]),
        (2, [mock.call("aaa")]),
        (3, []),
        (0, [mock.call("aaa"), mock.call("bbb"), mock.call("ccc")]),
    ],
)
@mock.patch("twindb_backup.os")
@mock.patch("twindb_backup.glob")
def test_delete_local_files(mock_glob, mock_os, keep, calls):
    mock_glob.glob.return_value = ["aaa", "bbb", "ccc"]

    delete_local_files("/foo", keep)
    mock_os.unlink.assert_has_calls(calls)
