import socket

import pytest
import time

from tests.integration.conftest import get_container
from twindb_backup import LOG


# noinspection PyShadowingNames
@pytest.yield_fixture
def backup_server(docker_client, container_network):

    bootstrap_script = '/twindb-backup/support/bootstrap/backup_server.sh'
    container = get_container(
        'backup_server',
        bootstrap_script,
        docker_client,
        container_network
    )

    timeout = time.time() + 30 * 60

    while time.time() < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex((container['ip'], 22)) == 0:
            break
        time.sleep(1)

    yield container

    if container:
        LOG.info('Removing container %s', container['Id'])
        docker_client.api.remove_container(container=container['Id'],
                                           force=True)

@pytest.fixture
def config_content_ssh():
    return """
[source]
backup_dirs={BACKUP_DIR}

[destination]
backup_destination=ssh
keep_local_path=/var/backup/local

[ssh]
ssh_user=root
ssh_key={PRIVATE_KEY}
backup_host={HOST_IP}
backup_dir=/tmp/backup

[retention]

# Remote retention policy

hourly_copies=24
daily_copies=7
weekly_copies=4
monthly_copies=12
yearly_copies=3

[retention_local]

# Local retention policy

hourly_copies=1
daily_copies=1
weekly_copies=0
monthly_copies=0
yearly_copies=0

[intervals]

# Run intervals

run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=yes
run_yearly=yes
"""
