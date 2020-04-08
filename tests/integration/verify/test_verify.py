from textwrap import dedent

from tests.integration.conftest import get_twindb_config_dir, docker_execute
from twindb_backup import LOG


def test_verify_on_master(
    master1, slave, storage_server, config_content_ssh, docker_client, rsa_private_key
):

    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"

    for cont in master1, slave:
        twindb_config_dir = get_twindb_config_dir(docker_client, cont["Id"])

        twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir

        my_cnf_path = "%s/my.cnf" % twindb_config_dir
        ssh_key_host = "%s/id_rsa" % twindb_config_dir
        ssh_key_guest = "/etc/twindb/id_rsa"

        contents = dedent(
            """
            [client]
            user=dba
            password=qwerty
            """
        )

        with open(my_cnf_path, "w") as my_cnf:
            my_cnf.write(contents)
            my_cnf.flush()

        with open(ssh_key_host, "w") as ssh_fd:
            ssh_fd.write(rsa_private_key)
            ssh_fd.flush()

        with open(twindb_config_host, "w") as fp:
            content = config_content_ssh.format(
                PRIVATE_KEY=ssh_key_guest,
                HOST_IP=storage_server["ip"],
                MY_CNF="/etc/twindb/my.cnf",
            )
            fp.write(content)
            fp.flush()

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    LOG.info(cout)

    assert ret == 0

    cmd = [
        "bash",
        "-c",
        "twindb-backup --config %s ls | grep /tmp/backup "
        "| grep mysql | sort | tail -1" % twindb_config_guest,
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    url = cout.strip()
    LOG.info(cout)
    assert ret == 0

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "verify",
        "mysql",
        url,
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    LOG.info(cout)
    assert ret == 0

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "verify",
        "mysql",
        "latest",
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    LOG.info(cout)
    assert ret == 0

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "verify",
        "mysql",
        "--hostname",
        "master1_1",
        "latest",
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    LOG.info(cout)
    assert ret == 0
