"""Tests that cover the FileSource().get_stream() method."""
import mock
import pytest

from twindb_backup.source.file_source import FileSource


@pytest.mark.parametrize(
    "tar_options, expected_command",
    [
        (None, ["tar", "cf", "-", "foo"]),
        ("--exclude-vcs-ignores", ["tar", "cf", "-", "--exclude-vcs-ignores", "foo"]),
        (
            "--exclude-vcs-ignores --exclude-ignore=FILE",
            ["tar", "cf", "-", "--exclude-vcs-ignores", "--exclude-ignore=FILE", "foo"],
        ),
    ],
)
@mock.patch("twindb_backup.source.file_source.Popen")
def test_get_stream(mock_popen, tar_options, expected_command):
    """Make sure tar command is built properly."""
    mock_proc = mock.Mock()
    mock_proc.communicate.return_value = None, None
    mock_proc.returncode = 0

    mock_popen.return_value = mock_proc

    src = FileSource("foo", "daily", tar_options=tar_options)
    with src.get_stream():
        pass

    mock_popen.assert_called_once_with(expected_command, stderr=-1, stdout=-1)
