import socket

import docker
import os
import pytest
import time
from docker.errors import DockerException, APIError
from docker.types import IPAMPool, IPAMConfig

from tests.integration import ensure_aws_creds
from twindb_backup import setup_logging, LOG
from twindb_backup.util import mkdir_p

try:
    NODE_IMAGE = os.environ['DOCKER_IMAGE']
except KeyError:
    raise EnvironmentError("""You must define the DOCKER_IMAGE environment 
    variable. Valid values are:
    * twindb/backup-test:centos-7
    * centos:centos6
    * debian:jessie
    * ubuntu:trusty
    * ubuntu:xenial
    """)

NETWORK_NAME = 'test_network'

setup_logging(LOG, debug=True)
ensure_aws_creds()


@pytest.fixture(scope="module")
def docker_client():
    for _ in xrange(5):
        try:
            return docker.DockerClient(version="auto")
        except DockerException as err:
            LOG.error(err)
            time.sleep(5)
    raise DockerException('Failed to get a docker client')


# noinspection PyShadowingNames
@pytest.fixture(scope="module")
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
@pytest.yield_fixture(scope="module")
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


def get_container(name, client, network, datadir,
                  bootstrap_script=None, last_n=1):
    api = client.api

    api.pull(NODE_IMAGE)
    cwd = os.getcwd()
    twindb_config_dir = cwd + '/env/twindb'
    mkdir_p(twindb_config_dir, mode=0755)
    host_config = api.create_host_config(
        binds={
            cwd: {
                'bind': '/twindb-backup',
                'mode': 'rw',
            },
            twindb_config_dir: {
                'bind': '/etc/twindb',
                'mode': 'rw',
            },
            datadir: {
                'bind': '/var/lib/mysql',
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

    container_hostname = '%s_%d' % (name, last_n)
    kwargs = {
        'image': NODE_IMAGE,
        'name': container_hostname,
        'ports': [22, 3306],
        'hostname': container_hostname,
        'host_config': host_config,
        'networking_config': networking_config,
        'volumes': ['/twindb-backup']
    }
    if bootstrap_script:
        kwargs['command'] = 'bash %s' % bootstrap_script
    container = api.create_container(**kwargs)
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
@pytest.yield_fixture(scope="module")
def master1(docker_client, container_network, tmpdir_factory):

    try:
        platform = os.environ['PLATFORM']
    except KeyError:
        raise EnvironmentError("""You must define environment variable PLATFORM.
        Allowed values are centos, debian and ubuntu""")

    bootstrap_script = '/twindb-backup/support/bootstrap/master/' \
                       '%s/master1.sh' % platform
    datadir = tmpdir_factory.mktemp('mysql')
    container = get_container(
        'master1',
        docker_client,
        container_network,
        str(datadir),
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
    raw_container.exec_run('bash -c "mysql -uroot mysql < %s"'
                           % privileges_file)
    docker_execute(docker_client, container['Id'], ['ls'])
    docker_execute(
        docker_client,
        container['Id'],
        ['bash', bootstrap_script]
    )
    yield container
    if container:
        LOG.info('Removing container %s', container['Id'])
        docker_client.api.remove_container(container=container['Id'],
                                           force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture(scope="module")
def master2(docker_client, container_network):

    bootstrap_script = '/twindb-backup/support/bootstrap/master2.sh'
    container = get_container(
        'master2',
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


def docker_execute(client, container_id, cmd):
    """Execute command in container

    :param client: Docker client class instance
    :type client: APIClient
    :param container_id: Container Id from a dictionary that get_container
        returns.
    :type container_id: str
    :param cmd: Command to execute
    :type cmd: str or list
    :return: A tuple with exit code and output.
    :rtype: tuple(int, str)
    """
    api = client.api
    executor = api.exec_create(container_id, cmd)
    exec_id = executor['Id']
    cout = api.exec_start(exec_id)
    ret = api.exec_inspect(exec_id)['ExitCode']
    return ret, cout
