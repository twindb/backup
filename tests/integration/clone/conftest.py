import os

import docker
import pytest
import time

from docker.errors import APIError
from docker.types import IPAMConfig, IPAMPool

from twindb_backup import LOG, setup_logging

NODE_IMAGE = 'centos:centos6'
NETWORK_NAME = 'test_network'

setup_logging(LOG, debug=True)


@pytest.fixture(scope='session')
def docker_client():
    return docker.DockerClient(version="auto")


# noinspection PyShadowingNames
@pytest.fixture(scope='session')
def node_image(docker_client):
    docker_client.api.pull(NODE_IMAGE)


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
    ipam_config = None

    for octet in xrange(16, 31):
        subnet = "172.%d.0.0/16" % octet
        network_params['subnet'] = subnet
        network_params['second_octet'] = octet
        try:
            ipam_pool = IPAMPool(
                subnet=subnet
            )
            ipam_config = IPAMConfig(
                pool_configs=[ipam_pool]
            )

        except APIError as err:
            if err.status_code == 409:
                LOG.warning('Subnet %s already exists', subnet)
                continue
            else:
                raise
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
        # command='bash /twindb-backup/support/clone/master%d.sh' % n
        command='/bin/sleep 36000'
    )
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
    yield container
    if container:
        LOG.info('Removing container %s', container['Id'])
        docker_client.api.remove_container(container=container['Id'],
                                           force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture
def master2(docker_client, container_network):

    container = _get_master(2, docker_client, container_network)
    yield container
    if container:
        LOG.info('Removing container %s', container['Id'])
        docker_client.api.remove_container(container=container['Id'],
                                           force=True)
