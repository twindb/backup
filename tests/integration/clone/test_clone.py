import time
from os import path as osp

from tests.integration.conftest import (
    assert_and_pause,
    docker_execute,
    get_twindb_config_dir,
    pause_test,
)
from twindb_backup import INTERVALS, LOG
from twindb_backup.backup import timeout
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


def test_clone(
    runner,
    master1,
    slave,
    docker_client,
    config_content_clone,
    client_my_cnf,
    rsa_private_key,
):

    twindb_config_dir = get_twindb_config_dir(docker_client, runner["Id"])
    twindb_config_host = osp.join(twindb_config_dir, "twindb-backup-1.cfg")
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"
    my_cnf_path = osp.join(twindb_config_dir, "my.cnf")

    private_key_host = osp.join(twindb_config_dir, "private_key")
    private_key_guest = "/etc/twindb/private_key"

    # Write config files locally. They're available on the guest
    # via a mounted /etc/twindb/
    with open(my_cnf_path, "w") as my_cnf:
        LOG.debug("Saving my.cnf in %s", my_cnf_path)
        my_cnf.write(client_my_cnf)

    with open(private_key_host, "w") as key_fd:
        LOG.debug("Saving private key in %s", private_key_host)
        key_fd.write(rsa_private_key)

    with open(twindb_config_host, "w") as fp:
        LOG.debug("Saving twindb config in %s", twindb_config_host)
        content = config_content_clone.format(
            PRIVATE_KEY=private_key_guest, MY_CNF="/etc/twindb/my.cnf"
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "clone",
        "mysql",
        "--replication-password",
        "qwerty",
        "%s:3306" % master1["ip"],
        "%s:3306" % slave["ip"],
    ]
    ret, cout = docker_execute(docker_client, runner["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    sql_master_2 = RemoteMySQLSource(
        {
            "ssh_host": slave["ip"],
            "ssh_user": "root",
            "ssh_key": private_key_guest,
            "mysql_connect_info": MySQLConnectInfo(
                my_cnf_path, hostname=slave["ip"]
            ),
            "run_type": INTERVALS[0],
            "backup_type": "full",
        }
    )

    with timeout(30):
        while True:
            with sql_master_2.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW SLAVE STATUS")
                    row = cursor.fetchone()
                    if (
                        row["Slave_IO_Running"] == "Yes"
                        and row["Slave_SQL_Running"] == "Yes"
                    ):

                        LOG.info("Replication is up and running")
                        return
    # noinspection PyUnreachableCode
    assert_and_pause(
        (False,), "Replication is not running after 30 seconds timeout"
    )
