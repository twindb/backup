import mock
import pytest

from twindb_backup.ssh.client import SshClient


@pytest.mark.parametrize(
    "exec_return, expected",
    [
        ("{root}\n", []),
        ("{root}\n{root}/bar.txt\n", ["{root}/bar.txt"]),
        # Some non-existent prefix
        ("", []),
    ],
)
@mock.patch.object(SshClient, "execute")
def test_list_files(mock_execute, exec_return, expected, tmpdir):

    root_dir = tmpdir.mkdir("foo")

    mock_execute.return_value = (exec_return.format(root=str(root_dir)), "")
    ssh = SshClient()
    check_result = []
    for x in expected:
        check_result.append(x.format(root=str(root_dir)))

    assert ssh.list_files(root_dir) == check_result


@pytest.mark.parametrize(
    "exec_return, expected",
    [
        ("\n", []),
        ("{root}/bar.txt\n", ["{root}/bar.txt"]),
        ("", []),
    ],
)
@mock.patch.object(SshClient, "execute")
def test_list_files_files_only_with_result(
    mock_execute, exec_return, expected, tmpdir
):

    root_dir = tmpdir.mkdir("foo")

    mock_execute.return_value = (exec_return.format(root=str(root_dir)), "")
    ssh = SshClient()
    check_result = []
    for x in expected:
        check_result.append(x.format(root=str(root_dir)))

    assert ssh.list_files(root_dir, files_only=True) == check_result


@pytest.mark.parametrize(
    "recursive, cmd",
    [
        (True, "bash -c 'if test -d {root} ; then find {root}; fi'"),
        (
            False,
            "bash -c 'if test -d {root} ; then find {root} -maxdepth 1; fi'",
        ),
    ],
)
@mock.patch.object(SshClient, "execute")
def test_list_files_recursive(mock_execute, recursive, cmd, tmpdir):

    root_dir = tmpdir.mkdir("foo")
    mock_execute.return_value = "", ""

    ssh = SshClient()
    ssh.list_files(root_dir, recursive=recursive)
    mock_execute.assert_called_once_with(cmd.format(root=str(root_dir)))


@pytest.mark.parametrize(
    "files_only, cmd",
    [
        (True, "bash -c 'if test -d {root} ; then find {root} -type f; fi'"),
        (False, "bash -c 'if test -d {root} ; then find {root}; fi'"),
    ],
)
@mock.patch.object(SshClient, "execute")
def test_list_files_files_only(mock_execute, files_only, cmd, tmpdir):

    root_dir = tmpdir.mkdir("foo")
    mock_execute.return_value = "", ""

    ssh = SshClient()
    ssh.list_files(root_dir, files_only=files_only, recursive=True)
    mock_execute.assert_called_once_with(cmd.format(root=str(root_dir)))
