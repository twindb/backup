from os import path as osp

from tests.integration.conftest import assert_and_pause, docker_execute


def check_either_file(docker_client, container_id, dst_dir, files_to_check):
    """Happy if at least one file from a files_to_check list exists."""
    cmd = [
        "bash",
        "-c",
        "||".join([f"test -f {osp.join(dst_dir, fl)}" for fl in files_to_check]),
    ]

    ret, cout = docker_execute(docker_client, container_id, cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)


def check_files_if_xtrabackup(docker_client, container_id, dst_dir, files_to_check):
    # MariaDB and Percona Server work with redo logs differently
    # If it's xtrabackup both ib_logfile1 must exist.
    for datadir_file in files_to_check:
        # See https://en.wikipedia.org/wiki/Material_conditional
        cmd = [
            "bash",
            "-c",
            f"! which xtrabackup || test -f {osp.join(dst_dir, datadir_file)}",
        ]
        print(cmd)
        ret, cout = docker_execute(docker_client, container_id, cmd)
        print(cout)
        assert_and_pause((ret == 0,), cout)
