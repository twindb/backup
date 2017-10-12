import os
import socket

import docker
import pytest
import time

from docker.errors import APIError, DockerException
from docker.types import IPAMConfig, IPAMPool

from twindb_backup import LOG, setup_logging

NODE_IMAGE = 'centos:centos6'
NETWORK_NAME = 'test_network'

setup_logging(LOG, debug=True)


@pytest.fixture(scope='session')
def docker_client():
    for _ in xrange(5):
        try:
            return docker.DockerClient(version="auto")
        except DockerException:
            time.sleep(5)
    raise DockerException('Failed to get a docker client')


# noinspection PyShadowingNames
@pytest.fixture(scope='session')
def node_image(docker_client):
    docker_client.api.pull(NODE_IMAGE)


def _ipam_config():
    for octet in xrange(16, 31):
        subnet = "172.%d.0.0/16" % octet
        try:
            ipam_pool = IPAMPool(
                subnet=subnet
            )
            ipam_config = IPAMConfig(
                pool_configs=[ipam_pool]
            )
            return ipam_config
        except APIError as err:
            if err.status_code == 409:
                LOG.info('Subnet %s already exists', subnet)
                continue
            else:
                raise


# noinspection PyShadowingNames
@pytest.yield_fixture(scope='session')
def container_network(docker_client):
    api = docker_client.api
    network = None
    network_params = {
        'NAME': NETWORK_NAME,
        'subnet': None,
        'second_octet': None
    }
    ipam_config = _ipam_config()

    subnet = ipam_config['Config'][0]['Subnet']
    network_params['subnet'] = subnet
    network_params['second_octet'] = int(subnet.split('.')[1])

    try:
        network = api.create_network(
            name=NETWORK_NAME,
            driver="bridge",
            ipam=ipam_config,
            check_duplicate=True
        )
        LOG.info('Created subnet %s', network_params['subnet'])
        LOG.debug(network)
    except APIError as err:
        if err.status_code == 500:
            LOG.info('Network %r already exists', network)
        else:
            raise

    yield network_params
    if network:
        api.remove_network(net_id=network['Id'])


def _get_master(n, client, network):
    """

    :param n: 1 or 2
    :return: Container
    """
    api = client.api

    api.pull(NODE_IMAGE)
    cwd = os.getcwd()
    host_config = api.create_host_config(
        binds={
            cwd: {
                'bind': '/twindb-backup',
                'mode': 'rw',
            }
        },
        dns=['8.8.8.8']
    )

    ip = '172.%d.3.%d' % (network['second_octet'], n)
    networking_config = api.create_networking_config({
        network['NAME']: api.create_endpoint_config(ipv4_address=ip)
    })

    LOG.debug(networking_config)

    container = api.create_container(
        image=NODE_IMAGE,
        name='master%d' % n,
        ports=[22, 3306],
        host_config=host_config,
        networking_config=networking_config,
        volumes=['/twindb-backup'],
        command='bash /twindb-backup/support/clone/master%d.sh' % n
        # command='/bin/sleep 36000'
    )
    container['ip'] = ip
    LOG.info('Created container %r', container)
    try:
        api.start(container['Id'])
        LOG.info('Started %r', container)

        return container
    except APIError as err:
        LOG.error(err)
        client.api.remove_container(container=container['Id'], force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture(scope='session')
def master1(docker_client, container_network):

    container = _get_master(1, docker_client, container_network)

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

    container = _get_master(2, docker_client, container_network)

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
ssh_key={PRIVATE_KEY}

[mysql]
mysql_defaults_file={MY_CNF}
"""
