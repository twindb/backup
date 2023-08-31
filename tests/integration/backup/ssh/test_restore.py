import os
from os import path as osp

from tests.integration.backup.conftest import check_either_file, check_files_if_xtrabackup
from tests.integration.conftest import assert_and_pause, docker_execute, get_twindb_config_dir
from twindb_backup import LOG


def test_restore(master1, storage_server, config_content_ssh, docker_client, rsa_private_key):

    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    ssh_key_host = "%s/id_rsa" % twindb_config_dir
    ssh_key_guest = "/etc/twindb/id_rsa"

    contents = """
[client]
user=dba
password=qwerty
"""
    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    with open(ssh_key_host, "w") as ssh_fd:
        ssh_fd.write(rsa_private_key)
    os.chmod(ssh_key_host, 0o600)

    with open(twindb_config_host, "w") as fp:
        content = config_content_ssh.format(
            PRIVATE_KEY=ssh_key_guest,
            HOST_IP=storage_server["ip"],
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "bash",
        "-c",
        "twindb-backup --config %s ls | grep /tmp/backup " "| grep mysql | sort | tail -1" % twindb_config_guest,
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    url = cout.strip()
    assert_and_pause((ret == 0,), cout)

    dst_dir = "/tmp/ssh_dest_restore/"
    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "restore",
        "mysql",
        url,
        "--dst",
        dst_dir,
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    cmd = ["find", dst_dir]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    for fl in ["backup-my.cnf", "ibdata1"]:
        cmd = ["test", "-f", osp.join(dst_dir, fl)]
        ret, cout = docker_execute(docker_client, master1["Id"], cmd)
        assert_and_pause((ret == 0,), cout)

    check_files_if_xtrabackup(
        docker_client,
        master1["Id"],
        dst_dir,
        ["xtrabackup_logfile"],
    )
    check_either_file(
        docker_client,
        master1["Id"],
        dst_dir,
        ["_config/etc/my.cnf", "_config/etc/mysql/my.cnf"],
    )
