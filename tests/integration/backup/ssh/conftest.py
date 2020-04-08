import socket

import pytest
import time

from tests.integration.conftest import get_container
from twindb_backup import LOG


# noinspection PyShadowingNames
@pytest.yield_fixture
def storage_server(docker_client, container_network):

    bootstrap_script = "/twindb-backup/support/bootstrap/storage_server.sh"
    container = get_container(
        "storage_server",
        docker_client,
        container_network,
        bootstrap_script=bootstrap_script,
        image="centos:centos7",
        last_n=2,
    )

    timeout = time.time() + 30 * 60

    while time.time() < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex((container["ip"], 22)) == 0:
            break
        time.sleep(1)

    yield container

    if container:
        LOG.info("Removing container %s", container["Id"])
        docker_client.api.remove_container(container=container["Id"], force=True)


@pytest.fixture
def config_content_ssh():
    return """
[source]
backup_mysql=yes

[destination]
backup_destination=ssh
keep_local_path=/var/backup/local

[mysql]
mysql_defaults_file={MY_CNF}
full_backup=daily

[ssh]
ssh_user=root
ssh_key={PRIVATE_KEY}
backup_host={HOST_IP}
backup_dir=/tmp/backup

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=no
run_yearly=yes

[retention]
hourly_copies=24
daily_copies=7
weekly_copies=1
monthly_copies=1
yearly_copies=1

[retention_local]
hourly_copies=1
daily_copies=1
weekly_copies=0
monthly_copies=0
yearly_copies=0
"""
