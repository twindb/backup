# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""

from multiprocessing import Process

from twindb_backup import INTERVALS, LOG
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.util import split_host_port


def check_root_my_cnf(src):
    """
    Return path on source server to root my.cnf
    :param src: Source server
    :type src: Ssh
    :return: Path to my.cnf
    :raise IOError: if my.cnf is not found
    """
    mysql_configs = [
        '/etc/my.cnf',
        '/etc/mysql/my.cnf'
    ]
    for mysql_config in mysql_configs:
        try:
            with src.get_stream(mysql_config):
                return mysql_config
        except SshDestinationError:
            continue
    raise IOError("Root my.cnf not found")


def clone_mysql(cfg, source, destination, netcat_port=9990):
    """Clone mysql backup of remote machine and stream it to slave"""
    src = RemoteMySQLSource({
        "ssh_connection_info": SshConnectInfo(
            host=split_host_port(source)[0],
            user=cfg.get('ssh', 'ssh_user'),
            key=cfg.get('ssh', 'ssh_key')
        ),
        "mysql_connect_info": MySQLConnectInfo(
            cfg.get('mysql', 'mysql_defaults_file')
        ),
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
    })
    dst = Ssh(
        ssh_connect_info=SshConnectInfo(
            host=split_host_port(destination)[0],
            user=cfg.get('ssh', 'ssh_user'),
            key=cfg.get('ssh', 'ssh_key')
        ),
    )
    if dst.list_files(src.datadir):
        LOG.error("Data dir is not empty: %s", src.datadir)
        exit(1)

    proc_netcat = Process(
        target=dst.netcat,
        args=(
            " | gunzip -c - | xbstream -x -C {datadir}".format(
                datadir=src.datadir
            ),
        ),
        kwargs={
            'port': netcat_port
        }
    )
    proc_netcat.start()
    src.clone(dest_host=destination, port=netcat_port)
    proc_netcat.join()
    check_root_my_cnf(Ssh(src.ssh_connection_info, "/", hostname=source))
