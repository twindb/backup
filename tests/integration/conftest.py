import socket

import docker
import os
import pytest
import time
from docker.errors import DockerException, APIError
from docker.types import IPAMPool, IPAMConfig

from tests.integration import ensure_aws_creds
from twindb_backup import setup_logging, LOG

try:
    NODE_IMAGE = os.environ['DOCKER_IMAGE']
except KeyError:
    NODE_IMAGE = 'centos:centos7'

NETWORK_NAME = 'test_network'

setup_logging(LOG, debug=True)
ensure_aws_creds()


@pytest.fixture
def docker_client():
    for _ in xrange(5):
        try:
            return docker.DockerClient(version="auto")
        except DockerException as err:
            LOG.error(err)
            time.sleep(5)
    raise DockerException('Failed to get a docker client')


# noinspection PyShadowingNames
@pytest.fixture
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
@pytest.yield_fixture
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


def get_container(name, bootstrap_script, client, network, last_n=1):
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

    ip = '172.%d.3.%d' % (network['second_octet'], last_n)
    networking_config = api.create_networking_config({
        network['NAME']: api.create_endpoint_config(ipv4_address=ip)
    })

    LOG.debug(networking_config)

    container = api.create_container(
        image=NODE_IMAGE,
        name='%s_%d' % (name, last_n),
        ports=[22, 3306],
        host_config=host_config,
        networking_config=networking_config,
        volumes=['/twindb-backup'],
        command='bash %s' % bootstrap_script
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
@pytest.yield_fixture
def master1(docker_client, container_network):

    platform = None
    try:
        platform = os.environ['PLATFORM']
    except KeyError:
        print("""You must define environment variable PLATFORM.
        Allowed values are centos, debian and ubuntu""")
        exit(-1)
    bootstrap_script = '/twindb-backup/support/bootstrap/master/' \
                       '%s/master1.sh' % platform
    container = get_container(
        'master1',
        bootstrap_script,
        docker_client,
        container_network,
        1
    )

    timeout = time.time() + 30 * 60

    while time.time() < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex((container['ip'], 3306)) == 0:
            break
        time.sleep(1)

    raw_container = docker_client.containers.get(container['Id'])
    privileges_file = "/twindb-backup/vagrant/environment/puppet/" \
                      "modules/profile/files/mysql_grants.sql"
    raw_container.exec_run('bash -c "mysql -uroot -pMyNewPass mysql < %s"'
                           % privileges_file)

    yield container
    if container:
        LOG.info('Removing container %s', container['Id'])
        docker_client.api.remove_container(container=container['Id'],
                                           force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture
def master2(docker_client, container_network):

    bootstrap_script = '/twindb-backup/support/bootstrap/master2.sh'
    container = get_container(
        'master2',
        bootstrap_script,
        docker_client,
        container_network,
        2
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

