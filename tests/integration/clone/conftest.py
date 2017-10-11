import docker
import pytest
import time
from docker.types import IPAMConfig, IPAMPool


NODE_IMAGE = 'centos:centos6'
NETWORK_NAME = 'test_network'


@pytest.fixture
def docker_client():
    return docker.DockerClient(version="auto")


# noinspection PyShadowingNames
@pytest.fixture(scope='session')
def node_image(docker_client):
    docker_client.api.pull(NODE_IMAGE)


# noinspection PyShadowingNames
@pytest.yield_fixture
def container_network(docker_client):
    api = docker_client.api

    ipam_pool = IPAMPool(
        subnet="172.25.0.0/16"
    )

    ipam_config = IPAMConfig(
        pool_configs=[ipam_pool]
    )

    network = api.create_network(
        name=NETWORK_NAME,
        driver="bridge",
        ipam=ipam_config
    )

    yield NETWORK_NAME

    api.remove_network(net_id=network['Id'])


# noinspection PyShadowingNames
@pytest.yield_fixture
def master1(docker_client, container_network):

    api = docker_client.api

    api.pull(NODE_IMAGE)
    host_config = api.create_host_config()

    networking_config = api.create_networking_config({
        container_network: api.create_endpoint_config(
            ipv4_address='172.25.3.1'
        )
    })

    container = api.create_container(
        image=NODE_IMAGE,
        name='master1',
        ports=[22, 3306],
        host_config=host_config,
        networking_config=networking_config)
    api.start(container['Id'])

    yield container

    time.sleep(3600)
    docker_client.api.remove_container(container=container['id'], force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture
def master2(docker_client, container_network):

    api = docker_client.api
    api.pull(NODE_IMAGE)

    host_config = api.create_host_config()

    networking_config = api.create_networking_config({
        container_network: api.create_endpoint_config(
            ipv4_address='172.25.3.2'
        )
    })

    container = api.create_container(
        image=NODE_IMAGE,
        name='master2',
        ports=[22, 3306],
        host_config=host_config,
        networking_config=networking_config)
    api.start(container['Id'])

    yield container

    time.sleep(3600)
    docker_client.api.remove_container(container=container['id'], force=True)
