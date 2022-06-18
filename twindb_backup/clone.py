"""
Module defines clone feature
"""
import hashlib
from contextlib import contextmanager
from multiprocessing import Process
from typing import Union

from twindb_backup import INTERVALS, LOG, MBSTREAM_BINARY, XBSTREAM_BINARY
from twindb_backup.configuration import TwinDBBackupConfig
from twindb_backup.destination.ssh import Ssh
from twindb_backup.exceptions import OperationError
from twindb_backup.source.mysql_source import MySQLClient, MySQLConnectInfo, MySQLFlavor, MySQLMasterInfo
from twindb_backup.source.remote_mariadb_source import RemoteMariaDBSource
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.util import split_host_port

MYSQL_SRC_MAP = {
    MySQLFlavor.MARIADB: RemoteMariaDBSource,
    MySQLFlavor.ORACLE: RemoteMySQLSource,
    MySQLFlavor.PERCONA: RemoteMySQLSource,
}


def get_src_by_vendor(
    vendor: MySQLFlavor,
    ssh_host: str,
    ssh_user: str,
    ssh_key_path: str,
    mysql_connect_info: MySQLConnectInfo,
    run_type: str,
):
    return MYSQL_SRC_MAP[vendor](
        {
            "ssh_host": ssh_host,
            "ssh_user": ssh_user,
            "ssh_key": ssh_key_path,
            "mysql_connect_info": mysql_connect_info,
            "run_type": run_type,
            "backup_type": "full",
        }
    )


def detect_xbstream(cfg: TwinDBBackupConfig, mysql_client: MySQLClient) -> str:
    """Guess what xbtream tool should be used.

    If a user specifies the xbstream via the config, it will be used.
    Otherwise, use appropriate tool for the MySQL flavor.

    :param cfg: TwinDB config instance.
    :type cfg: TwinDBBackupConfig
    :param mysql_client: MySQL client instance connected to the source.
    :type mysql_client: MySQLClient
    :return: String, a path to xbstream that will be run on the destination
        to accept and extract XtraBackup stream.
    :rtype: str
    """
    return cfg.mysql.xbstream_binary or (
        MBSTREAM_BINARY if mysql_client.server_vendor is MySQLFlavor.MARIADB else XBSTREAM_BINARY
    )


def get_dst(cfg: TwinDBBackupConfig, destination: str) -> Ssh:
    """Prepare destination object.

    :param cfg: TwinDB Backup config.
    :type cfg: TwinDBBackupConfig
    :param destination: A host:port couple e.g. ``slave:3306``, the recipient.
    :type destination: str
    :return: Destination Ssh object.
    :rtype: Ssh
    """
    return Ssh(
        "/tmp",
        ssh_host=split_host_port(destination)[0],
        ssh_user=cfg.ssh.user,
        ssh_key=cfg.ssh.key,
    )


def get_src(
    cfg: TwinDBBackupConfig, mysql_client: MySQLClient, source: str
) -> Union[RemoteMariaDBSource, RemoteMySQLSource]:
    """Prepare source object.

    :param cfg: TwinDB Backup config.
    :type cfg: TwinDBBackupConfig
    :param mysql_client: MySQL client connected to the source.
    :type mysql_client: MySQLClient
    :param source: A host:port couple e.g. ``slave:3306``, the recipient.
    :type source: str
    :return: Source object
    :rtype: RemoteMariaDBSource, RemoteMySQLSource
    """
    return get_src_by_vendor(
        mysql_client.server_vendor,
        split_host_port(source)[0],
        cfg.ssh.user,
        cfg.ssh.key,
        MySQLConnectInfo(cfg.mysql.defaults_file, hostname=split_host_port(source)[0]),
        INTERVALS[0],
    )


def clone_mysql(
    cfg: TwinDBBackupConfig,
    source: str,
    destination: str,
    replication_user: str,
    replication_password: str,
    netcat_port=9990,
    compress=False,
):
    """Clone a MySQL instance from the remote server on ``source``
    to the remote server on the ``destination``.

    :param cfg: TwinDB Backup tool config.
    :type cfg: TwinDBBackupConfig
    :param source: A host:port couple e.g. ``master:3306``, the donor.
    :type source: str
    :param destination: A host:port couple e.g. ``slave:3306``, the recipient.
    :type destination: str
    :param replication_user: Username for the replication.
    :type replication_user: str
    :param replication_password: Password the replication.
    :type replication_password: str
    :param netcat_port: A beginning of the TCP ports range to be used
        by netcat for the stream transfer.
    :type netcat_port: int
    :param compress: Whether compress the stream or not.
        ``gzip`` will be used if yes.
    :type compress: bool
    """
    LOG.debug("Remote MySQL Source: %s", split_host_port(source)[0])
    LOG.debug("MySQL defaults: %s", cfg.mysql.defaults_file)
    LOG.debug("SSH username: %s", cfg.ssh.user)
    LOG.debug("SSH key: %s", cfg.ssh.key)
    mysql_client = MySQLClient(cfg.mysql.defaults_file, hostname=split_host_port(source)[0])
    src = get_src(cfg, mysql_client, source)
    xbstream_binary = detect_xbstream(cfg, mysql_client)

    LOG.debug("SSH destination: %s", split_host_port(destination)[0])
    LOG.debug("SSH username: %s", cfg.ssh.user)
    LOG.debug("SSH key: %s", cfg.ssh.key)
    dst = get_dst(cfg, destination)
    datadir = src.datadir
    # STEP 1: Ensure a destination directory is empty
    LOG.debug("MySQL datadir: %s", datadir)
    step_ensure_empty_directory(dst, datadir)

    # STEP 2: Start netcat on the destination
    with step_run_remote_netcat(compress, datadir, dst, netcat_port, xbstream_binary) as port_final:
        # STEP 3: Start XtraBackup on the source
        # and stream it to the destination
        step_clone_source(src, split_host_port(destination)[0], port_final, compress)

    # STEP 4: Copy a MySQL configuration to the destination
    step_clone_mysql_config(src, dst)

    LOG.debug("Remote MySQL destination: %s", split_host_port(destination)[0])
    LOG.debug("MySQL defaults: %s", cfg.mysql.defaults_file)
    LOG.debug("SSH username: %s", cfg.ssh.user)
    LOG.debug("SSH key: %s", cfg.ssh.key)

    dst_mysql = MYSQL_SRC_MAP[mysql_client.server_vendor](
        {
            "ssh_host": split_host_port(destination)[0],
            "ssh_user": cfg.ssh.user,
            "ssh_key": cfg.ssh.key,
            "mysql_connect_info": MySQLConnectInfo(
                cfg.mysql.defaults_file,
                hostname=split_host_port(destination)[0],
            ),
            "run_type": INTERVALS[0],
            "backup_type": "full",
        }
    )
    # STEP 5: Apply the REDO log on the destination
    binlog, position = dst_mysql.apply_backup(datadir)

    LOG.debug("Binlog coordinates: (%s, %d)", binlog, position)

    # STEP 6: Start MySQL on the destination
    LOG.debug("Starting MySQL on the destination")
    step_start_mysql_service(dst)

    # STEP 7: Configure replication on the destination
    step_configure_replication(
        dst_mysql,
        MySQLMasterInfo(
            host=split_host_port(source)[0],
            port=split_host_port(source)[1],
            user=replication_user,
            password=replication_password,
            binlog=binlog,
            binlog_pos=position,
        ),
    )


def step_ensure_empty_directory(destination: Ssh, path: str):
    """Check if a directory on the remote server is empty.
    If not raise an exception.

    :param destination: Remote server.
    :param path: Path to the directory to check.
    :raise OperationError: If the directory is not empty.
    """
    if destination.list_files(path):
        raise OperationError(f"Destination directory {path} is not empty")


@contextmanager
def step_run_remote_netcat(
    compress: bool,
    datadir,
    dst,
    netcat_port,
    xbstream_path,
):
    """Run netcat with xbstream on a remote server via SSH.
    The function will find a free TCP port for the transfer.
    It will start looking from a given ``netcat_port``
    up to max TCP port 65535. The function will yield the found port number.

    :param compress: Compress the stream or not.
    :type compress: bool
    :param datadir: The directory on the remote destination server
        where to uncompress the stream.
    :type datadir: str
    :param dst:
    :param netcat_port:
    :param xbstream_path:
    :return:
    """
    LOG.debug("Looking for an available TCP port for netcat.")
    while netcat_port < 65535:
        if dst.ensure_tcp_port_listening(netcat_port, wait=False):
            netcat_port += 1
        else:
            LOG.debug("Will use port %d for streaming.", netcat_port)
            break

    netcat_cmd = f"{xbstream_path} -x -C {datadir} 2> /tmp/xbstream.err"
    if compress:
        netcat_cmd = f"gunzip -c - | {netcat_cmd}"

    proc_netcat = Process(target=dst.netcat, args=(netcat_cmd,), kwargs={"port": netcat_port})
    LOG.debug("Starting netcat on the destination.")
    proc_netcat.start()
    nc_wait_timeout = 10
    if not dst.ensure_tcp_port_listening(netcat_port, wait_timeout=nc_wait_timeout):
        LOG.error(
            "netcat on the destination is not ready after %d seconds.",
            nc_wait_timeout,
        )
        proc_netcat.terminate()
        exit(1)
    LOG.debug("netcat on the destination is ready on port %d.", netcat_port)
    yield netcat_port
    proc_netcat.join()


def step_clone_source(
    source: Union[RemoteMariaDBSource, RemoteMySQLSource],
    destination_hostname: str,
    netcat_port: int,
    compress_flag: bool,
):
    """Start cloning the source to the destination.

    :param source: An instance with the source class
        that will be used as a donor.
    :param destination_hostname: A hostname where the clone will be set up.
    :param netcat_port: A TCP port for the netcat transfer.
    :param compress_flag: Whether compress the stream or not.
    """
    source.clone(
        dest_host=destination_hostname,
        port=netcat_port,
        compress=compress_flag,
    )


def step_clone_mysql_config(source: Union[RemoteMariaDBSource, RemoteMySQLSource], destination: Ssh):
    """
    Copy MySQL config from the source MySQL server to the destination server.
    """
    LOG.debug("Copying MySQL config to the destination")
    source.clone_config(destination)


def step_start_mysql_service(dst):
    """Start MySQL service

    :param dst: Destination server
    :type dst: Ssh
    """
    service_name = _get_mysql_service_name(dst)
    dst.execute_command(f"systemctl start {service_name}")


def step_configure_replication(
    destination: Union[RemoteMariaDBSource, RemoteMySQLSource],
    master_data: MySQLMasterInfo,
):
    """
    Configure replication on the remote MySQL server.

    :param destination: MySQL server, a replica.
    :param master_data: Replication parameters: master hostname,
        user, password, etc.
    """
    LOG.debug("Setting up replication.")
    LOG.debug("Master host: %s", master_data.host)
    LOG.debug("Replication user: %s", master_data.user)
    LOG.debug(
        "Replication password(sha256): %s",
        hashlib.sha256(master_data.password.encode()).hexdigest().upper(),
    )
    destination.setup_slave(master_data)


def _get_mysql_service_name(remote_server: Ssh) -> str:
    """
    Detect how MySQL service is called - mysql or mysqld

    :param remote_server:
    :return: a string ``mysql`` or ``mysqld``.
    :raise OperationError: if neither service is present on the remote server.
    """
    for candidate in ["mysql", "mysqld", "mariadb"]:
        count = int(
            remote_server.execute_command(
                f"systemctl list-units --full -all " f"| grep -F '{candidate}.service' | wc -l"
            )[0].strip()
        )
        if count == 1:
            return candidate
    raise OperationError(f"Could not detect name of the MySQL service on {remote_server.host}")
