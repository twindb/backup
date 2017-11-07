import socket

import pytest
import time


# noinspection PyShadowingNames
from tests.integration.conftest import get_container
from twindb_backup import LOG


@pytest.yield_fixture(scope='session')
def master1(docker_client, container_network):

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
def master2(docker_client, container_network):

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
def config_content_clone():
    return """

[ssh]
ssh_user=root
ssh_key={PRIVATE_KEY}z

[mysql]
mysql_defaults_file={MY_CNF}
"""
