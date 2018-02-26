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
    * twindb/backup-test:centos-6
    * twindb/backup-test:jessie
    * twindb/backup-test:trusty
    * twindb/backup-test:xenial
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
                  bootstrap_script=None, last_n=1,
                  twindb_config_dir=None):
    api = client.api

    api.pull(NODE_IMAGE)
    cwd = os.getcwd()
    LOG.debug('Current directory: %s', cwd)
    LOG.debug('TwinDB config directory: %s', twindb_config_dir)
    mkdir_p(twindb_config_dir, mode=0755)
    binds = {
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
    }
    if twindb_config_dir:
        binds[twindb_config_dir] = {
            'bind': '/etc/twindb',
            'mode': 'rw',
        }
    host_config = api.create_host_config(
        binds=binds,
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
        raise EnvironmentError("""The environment variable PLATFORM 
        must be defined. Allowed values are:
        * centos
        * debian
        * ubuntu
        """)

    bootstrap_script = '/twindb-backup/support/bootstrap/master/' \
                       '%s/master1.sh' % platform
    datadir = tmpdir_factory.mktemp('mysql')
    twindb_config_dir = tmpdir_factory.mktemp('twindb')
    container = get_container(
        'master1',
        docker_client,
        container_network,
        str(datadir),
        twindb_config_dir=str(twindb_config_dir),
        last_n=1
    )

    timeout = time.time() + 30 * 60
    LOG.info('Waiting until port TCP/3306 becomes available')
    while time.time() < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex((container['ip'], 3306)) == 0:
            break
        time.sleep(1)
        LOG.info('Still waiting')

    privileges_file = "/twindb-backup/vagrant/environment/puppet/" \
                      "modules/profile/files/mysql_grants.sql"
    cmd = ["bash", "-c",
           "mysql -uroot mysql < %s" % privileges_file]
    ret, cout = docker_execute(docker_client, container['Id'], cmd)

    print(cout)
    assert ret == 0

    ret, _ = docker_execute(docker_client, container['Id'], ['ls'])
    assert ret == 0

    ret, cout = docker_execute(
        docker_client,
        container['Id'],
        ['bash', bootstrap_script]
    )
    print(cout)
    assert ret == 0

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


def get_twindb_config_dir(client, container_id):
    """Read hostconfig of a container and return directory on a host server
    that is mounted as /etc/twindb in the container

    :param client: Docker client class instance
    :type client: APIClient
    :param container_id: container id. can be something like c870459a6724 or
        container name like builder_xtrabackup
    :return: directory on the host machine
    :rtype: str
    """
    api = client.api
    host_config = api.inspect_container(container_id)['HostConfig']
    binds = host_config['Binds']
    for bind_pair in binds:
        print(bind_pair)
        bind = bind_pair.split(':')
        host_dir = bind[0]
        guest_dir = bind[1]
        if guest_dir == '/etc/twindb':
            return host_dir
    raise RuntimeError('Could not find binding for /etc/twindb')
