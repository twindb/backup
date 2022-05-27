import json
import os

from tests.integration.conftest import (
    get_twindb_config_dir,
    docker_execute,
    assert_and_pause,
)


def test_backup(
    master1, storage_server, config_content_ssh, docker_client, rsa_private_key
):

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
        ssh_fd.write(rsa_private_key.strip())

    with open(twindb_config_host, "w") as fp:
        content = config_content_ssh.format(
            PRIVATE_KEY=ssh_key_guest,
            HOST_IP=storage_server["ip"],
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)
    os.chmod(ssh_key_host, 0o600)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    cmd = ["twindb-backup", "--config", twindb_config_guest, "status"]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert ret == 0
    status = json.loads(cout)
    assert len(status["hourly"]) == 1

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    cmd = ["test", "-d", "/tmp/backup"]

    ret, cout = docker_execute(docker_client, storage_server["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    dir_path = "/var/backup/local/master1_1/hourly/mysql"
    cmd = ["bash", "-c", "ls %s | wc -l" % dir_path]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd, tty=True)
    assert_and_pause((ret == 0,), cout)
    assert_and_pause(("1" in cout,), cout)

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
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    dir_path = "/var/backup/local/master1_1/daily/mysql"
    cmd = ["bash", "-c", "ls %s | wc -l" % dir_path]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd, tty=True)
    print(cout)
    assert_and_pause((ret == 0,), cout)
    assert_and_pause(("1" in cout,), cout)
