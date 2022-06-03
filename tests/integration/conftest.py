import os
import socket
import time
from os import path as osp
from textwrap import dedent

import docker
import pytest
from docker.errors import APIError, DockerException
from docker.types import IPAMConfig, IPAMPool

from tests.integration import ensure_aws_creds
from twindb_backup import LOG, setup_logging
from twindb_backup.util import mkdir_p

SUPPORTED_IMAGES = [
    "twindb/backup-test:centos-7",
    "twindb/backup-test:bionic",
    "twindb/backup-test:focal",
]


try:
    NODE_IMAGE = os.environ["DOCKER_IMAGE"]
except KeyError:
    raise EnvironmentError(
        f"You must define the DOCKER_IMAGE environment variable. "
        f"Valid values are: \n{os.linesep.join(SUPPORTED_IMAGES)}"
    )

NETWORK_NAME = "test_network"

setup_logging(LOG, debug=True)
ensure_aws_creds()


def get_platform_from_image(image):
    if "centos" in image:
        return "centos"
    elif any(
        (
            ("jessie" in image),
            ("stretch" in image),
        )
    ):
        return "debian"
    elif any(
        (
            ("trusty" in image),
            ("xenial" in image),
            ("bionic" in image),
            ("focal" in image),
        )
    ):
        return "ubuntu"
    else:
        raise EnvironmentError(
            f"Cannot guess platform from docker image name {image}"
        )


@pytest.fixture
def client_my_cnf():
    return dedent(
        """
        [client]
        user=dba
        password=qwerty
        """
    )


@pytest.fixture
def rsa_private_key():
    return dedent(
        """
        -----BEGIN RSA PRIVATE KEY-----
        MIIEoAIBAAKCAQEAyXxAjPShNGAedbaEtltFI6A7RlsyI+4evxTq6uQrgbJ6Hm+p
        HBXshXQYXDyVjvytaM+6GKF+r+6+C+6Wc5Xz4lLO/ZiSCdPbyEgqw1JoHrgPNpc6
        wmCtjJExxjzvpwSVgbZg3xOdqW1y+TyqeUkXEg/Lm4VZhN1Q/KyGCgBlWuAXoOYR
        GhaNWqcnr/Wn5YzVHAx2yJNrurtKLVYVMIkGcN/6OUaPpWqKZLaXiK/28PSZ5GdT
        DmxRg4W0pdyGEYQndpPlpLF4w5gNUEhVZM8hWVE29+DIW3XXVYGYchxmkhU7wrGx
        xZR+k5AT+7g8VspVS8zNMXM9Z27w55EQuluNMQIBIwKCAQAzz35QIaXLo7APo/Y9
        hS8JKTPQQ1YJPTsbMUO4vlRUjPrUoF6vc1oTsCOFbqoddCyXS1u9MNdvEYFThn51
        flSn6WhtGJqU0BPxrChA2q0PNqTThfkqqyVQCBQdCFrhzfqPEaPhl1RtZUlzSh01
        IWxVGgEn/bfu9xTTQk5aV9+MZQ2XKe4BGzpOZMI/B7ivRCcthEwMTx92opr52bre
        4t7DahVLN/2Wu4lxajDzCaKXpjMuL76lFov0mZZN7S8whH5xSx1tpapHqsCAwfLL
        k49lDdR8aN6oqoeK0e9w//McIaKxN2FVxD4bcuXiQTjihx+QwQOLmlHSRDKhTsYg
        4Q5bAoGBAOgVZM2eqC8hNl5UH//uuxOeBKqwz7L/FtGemNr9m0XG8N9yE/K7A5iX
        6EDvDyVI51IlIXdxfK8re5yxfbJ4YevenwdEZZ2O8YRrVByJ53PV9CcVeWL4p6f/
        I56sYyDfXcnDTEOVYY0mCfYUfUcSb1ExpuIU4RvuQJg6tvbdxD9FAoGBAN4/pVCT
        krRd6PJmt6Dbc2IF6N09OrAnLB3fivGztF5cp+RpyqZK4ve+akLoe1laTg7vNtnF
        l/PZtM9v/VT45hb70MFEHO+sKvGa5Yimxkb6YCriJOcLxTysSgFHKz7v+8BqqoHi
        qY4fORGwPVDv28I8jKRvcuNHendV/Rdcuk79AoGAd1t1q5NscAJzu3u4r4IXEWc1
        mZzClpHROJq1AujTgviZInUu1JqxZGthgHrx2KkmggR3nIOB86/2bdefut7TRhq4
        L5+Et24VzxKgSTD6sJnrR0zfV3iQvMxbdizFRBsaSoGyMWLEdHn2fo4xzMem9o6Q
        VwNsdMOsMQhA1rsxuiMCgYBr8wcnIxte68jqxC1OIXKOsmnKi3RG7nSDidXF2vE1
        JbCiJMGD+Hzeu5KyyLDw4rgzI7uOWKjkJ+obnMuBCy3t6AZPPlcylXPxsaKwFn2Q
        MHfaUJWUyzPqRQ4AnukekdINAJv18cAR1Kaw0fHle9Ej1ERP3lxfw6HiMRSHsLJD
        nwKBgCIXVhXCDaXOOn8M4ky6k27bnGJrTkrRjHaq4qWiQhzizOBTb+7MjCrJIV28
        8knW8+YtEOfl5R053SKQgVsmRjjDfvCirGgqC4kSAN4A6MD+GNVXZVUUjAUBVUbU
        8Wt4BxW6kFA7+Su7n8o4DxCqhZYmK9ZUhNjE+uUhxJCJaGr4
        -----END RSA PRIVATE KEY-----
        """
    ).strip()


@pytest.fixture(scope="module")
def docker_client():
    for _ in range(5):
        try:
            return docker.DockerClient(
                version="auto",
                timeout=600,
            )
        except DockerException as err:
            LOG.error(err)
            time.sleep(5)
    raise DockerException("Failed to get a docker client")


# noinspection PyShadowingNames
@pytest.fixture(scope="module")
def node_image(docker_client):
    docker_client.api.pull(NODE_IMAGE)


def _ipam_config():
    for octet in range(16, 31):
        subnet = "172.%d.0.0/16" % octet
        try:
            ipam_pool = IPAMPool(subnet=subnet)
            ipam_config = IPAMConfig(pool_configs=[ipam_pool])
            return ipam_config
        except APIError as err:
            if err.status_code == 409:
                LOG.info("Subnet %s already exists", subnet)
                continue
            else:
                raise


# noinspection PyShadowingNames
@pytest.yield_fixture(scope="module")
def container_network(docker_client):
    api = docker_client.api
    network = None
    network_params = {
        "NAME": NETWORK_NAME,
        "subnet": None,
        "second_octet": None,
    }
    ipam_config = _ipam_config()

    subnet = ipam_config["Config"][0]["Subnet"]
    network_params["subnet"] = subnet
    network_params["second_octet"] = int(subnet.split(".")[1])

    try:
        network = api.create_network(
            name=NETWORK_NAME,
            driver="bridge",
            ipam=ipam_config,
            check_duplicate=True,
        )
        LOG.info("Created subnet %s", network_params["subnet"])
        LOG.debug(network)
    except APIError as err:
        if err.status_code == 500:
            LOG.info("Network %r already exists", network)
        else:
            raise

    yield network_params
    if network:
        api.remove_network(net_id=network["Id"])


def get_container(
    name,
    client,
    network,
    datadir=None,
    bootstrap_script=None,
    last_n=1,
    twindb_config_dir=None,
    image=NODE_IMAGE,
):
    api = client.api

    api.pull(image)
    cwd = os.getcwd()
    LOG.debug("Current directory: %s", cwd)

    binds = {
        cwd: {
            "bind": "/twindb-backup",
            "mode": "rw",
        }
    }
    if twindb_config_dir:
        LOG.debug("TwinDB config directory: %s", twindb_config_dir)
        mkdir_p(twindb_config_dir, mode=0o755)
        binds[twindb_config_dir] = {
            "bind": "/etc/twindb",
            "mode": "rw",
        }
    if datadir:
        binds[datadir] = {
            "bind": "/var/lib/mysql",
            "mode": "rw",
        }
    host_config = api.create_host_config(
        binds=binds, dns=["8.8.8.8", "208.67.222.222", "208.67.220.220"]
    )

    ip = "172.%d.3.%d" % (network["second_octet"], last_n)
    networking_config = api.create_networking_config(
        {network["NAME"]: api.create_endpoint_config(ipv4_address=ip)}
    )

    LOG.debug(networking_config)

    container_hostname = "%s_%d" % (name, last_n)
    kwargs = {
        "image": image,
        "name": container_hostname,
        "ports": [22, 3306],
        "hostname": container_hostname,
        "host_config": host_config,
        "networking_config": networking_config,
        "volumes": ["/twindb-backup"],
        "environment": {},
    }
    try:
        kwargs["environment"] = {"DEV": os.environ["DEV"]}
    except KeyError:
        pass
    if bootstrap_script:
        kwargs["command"] = "bash %s" % bootstrap_script
    container = api.create_container(**kwargs)
    container["ip"] = ip
    LOG.info("Created container %r", container)
    try:
        api.start(container["Id"])
        LOG.info("Started %r", container)

        return container
    except APIError as err:
        LOG.error(err)
        client.api.remove_container(container=container["Id"], force=True)


# noinspection PyShadowingNames
@pytest.yield_fixture(scope="module")
def master1(docker_client, container_network, tmpdir_factory):

    platform = (
        os.environ["PLATFORM"]
        if "PLATFORM" in os.environ
        else get_platform_from_image(os.environ["DOCKER_IMAGE"])
    )

    bootstrap_script = osp.join(
        osp.sep,
        "twindb-backup",
        "support",
        "bootstrap",
        "master",
        platform,
        "master1.sh",
    )
    datadir = tmpdir_factory.mktemp("mysql")
    twindb_config_dir = tmpdir_factory.mktemp("twindb")
    container = get_container(
        "master1",
        docker_client,
        container_network,
        str(datadir),
        twindb_config_dir=str(twindb_config_dir),
        last_n=1,
    )
    try:
        timeout = time.time() + 30 * 60
        LOG.info("Waiting until port TCP/3306 becomes available")
        while time.time() < timeout:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex((container["ip"], 3306)) == 0:
                break
            time.sleep(1)
            LOG.info("Still waiting")

        LOG.info("Port TCP/3306 is ready")

        privileges_file = osp.join(
            os.sep,
            "twindb-backup",
            "vagrant",
            "environment",
            "puppet",
            "modules",
            "profile",
            "files",
            "mysql_grants.sql",
        )
        cmd = ["bash", "-c", f"mysql -uroot mysql < {privileges_file}"]
        ret, cout = docker_execute(docker_client, container["Id"], cmd)

        print(cout)
        assert_and_pause((ret == 0,), cout)

        ret, _ = docker_execute(docker_client, container["Id"], ["ls"])
        assert ret == 0

        ret, cout = docker_execute(
            docker_client, container["Id"], ["bash", bootstrap_script]
        )
        print(cout)
        assert_and_pause((ret == 0,), cout)

        yield container

    finally:
        if container:
            LOG.info("Removing container %s", container["Id"])
            docker_client.api.remove_container(
                container=container["Id"], force=True
            )


# noinspection PyShadowingNames
@pytest.yield_fixture(scope="module")
def slave(docker_client, container_network, tmpdir_factory):
    platform = get_platform_from_image(os.environ["DOCKER_IMAGE"])
    bootstrap_script = osp.join(
        osp.sep,
        "twindb-backup",
        "support",
        "bootstrap",
        "master",
        platform,
        "slave.sh",
    )

    separator_pos = NODE_IMAGE.find(":")
    image_name = (
        NODE_IMAGE[: separator_pos + 1]
        + "slave_"
        + NODE_IMAGE[separator_pos + 1 :]
    )
    datadir = tmpdir_factory.mktemp("mysql")
    twindb_config_dir = tmpdir_factory.mktemp("twindb")
    container = get_container(
        "slave",
        docker_client,
        container_network,
        str(datadir),
        twindb_config_dir=str(twindb_config_dir),
        last_n=2,
        image=image_name,
    )
    try:
        timeout = time.time() + 30 * 60
        LOG.info("Waiting until port TCP/22 becomes available")
        while time.time() < timeout:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex((container["ip"], 22)) == 0:
                break
            time.sleep(1)
            LOG.info("Still waiting")
        LOG.info("Port TCP/22 is ready")
        ret, cout = docker_execute(docker_client, container["Id"], ["ls"])
        assert_and_pause((ret == 0,), cout)

        ret, cout = docker_execute(
            docker_client, container["Id"], ["bash", bootstrap_script]
        )
        print(cout)
        assert_and_pause((ret == 0,), cout)

        yield container

    finally:

        LOG.info("Removing container %s", container["Id"])
        docker_client.api.remove_container(
            container=container["Id"], force=True
        )


# noinspection PyShadowingNames
@pytest.yield_fixture(scope="module")
def runner(docker_client, container_network, tmpdir_factory):
    platform = get_platform_from_image(os.environ["DOCKER_IMAGE"])
    bootstrap_script = osp.join(
        osp.sep,
        "twindb-backup",
        "support",
        "bootstrap",
        "master",
        platform,
        "master1.sh",
    )

    datadir = tmpdir_factory.mktemp("mysql")
    twindb_config_dir = tmpdir_factory.mktemp("twindb")
    container = get_container(
        name="runner",
        client=docker_client,
        network=container_network,
        bootstrap_script=bootstrap_script,
        last_n=3,
        twindb_config_dir=str(twindb_config_dir),
        datadir=datadir,
    )
    try:
        ret, _ = docker_execute(docker_client, container["Id"], ["ls"])
        assert ret == 0

        ret, cout = docker_execute(
            docker_client, container["Id"], ["bash", bootstrap_script]
        )
        print(cout)
        assert_and_pause((ret == 0,), cout)

        yield container

    finally:

        LOG.info("Removing container %s", container["Id"])
        docker_client.api.remove_container(
            container=container["Id"], force=True
        )


def docker_execute(client, container_id, cmd, tty=False):
    """Execute command in container

    :param client: Docker client class instance
    :type client: APIClient
    :param container_id: Container Id from a dictionary that get_container
        returns.
    :type container_id: str
    :param cmd: Command to execute
    :type cmd: str or list
    :return: A tuple with exit code and output.
    :param tty: Using pseudo-TTY
    :type tty: bool
    :rtype: tuple(int, str)
    """
    cont = client.containers.get(container_id)
    LOG.debug("Running(on %s) %s", cont.name, " ".join(cmd))
    api = client.api
    executor = api.exec_create(container_id, cmd, tty=tty)
    exec_id = executor["Id"]
    cout = api.exec_start(exec_id)
    ret = api.exec_inspect(exec_id)["ExitCode"]
    return ret, cout.decode("utf-8")


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
    host_config = api.inspect_container(container_id)["HostConfig"]
    binds = host_config["Binds"]
    for bind_pair in binds:
        print(bind_pair)
        bind = bind_pair.split(":")
        host_dir = bind[0]
        guest_dir = bind[1]
        if guest_dir == "/etc/twindb":
            return host_dir
    raise RuntimeError("Could not find binding for /etc/twindb")


def pause_test(msg):
    """Pause"""
    try:
        if os.environ["PAUSE_TEST"]:
            LOG.debug("Test paused")
            LOG.debug(msg)
            import time

            time.sleep(36000)
    except KeyError:
        LOG.debug(
            "Define the PAUSE_TEST environment variable if you'd like to pause the test"
        )
        LOG.debug("export PAUSE_TEST=1")
        pass


def assert_and_pause(condition, msg):
    try:
        assert all(condition), msg
    except AssertionError as err:
        LOG.error(err)
        pause_test(msg)
        raise err
