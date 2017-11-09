import socket

import pytest
import time

from tests.integration.conftest import get_container
from twindb_backup import LOG


# noinspection PyShadowingNames
@pytest.yield_fixture
def instance1(docker_client, container_network):

    container = get_container(1, docker_client, container_network)

    timeout = time.time() + 30 * 60

    while time.time() < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex((container['ip'], 3306)) == 0:
            break
        time.sleep(1)

    raw_container = docker_client.containers.get('master1')
    privileges_file = "/twindb-backup/vagrant/environment/puppet/" \
                      "modules/profile/files/mysql_grants.sql"
    raw_container.exec_run('bash -c "mysql mysql < %s"'
                           % privileges_file)

    yield container
    if container:
        LOG.info('Removing container %s', container['Id'])
        docker_client.api.remove_container(container=container['Id'],
                                           force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture
def instance2(docker_client, container_network):

    container = get_container(2, docker_client, container_network)

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
# backup destination can be ssh or s3
backup_destination=ssh
keep_local_path=/var/backup/local

[ssh]
ssh_user=root
ssh_key={PRIVATE_KEY}
backup_host={HOST_IP}
backup_dir=/tmp/backup

[mysql]
mysql_defaults_file={MY_CNF}
full_backup=daily

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
