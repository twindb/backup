# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""
import ConfigParser
from multiprocessing import Process

from pymysql import OperationalError

from twindb_backup import INTERVALS, LOG
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.ssh.exceptions import SshClientException
from twindb_backup.util import split_host_port


def clone_mysql(cfg, source, destination, netcat_port=9990):
    """Clone mysql backup of remote machine and stream it to slave"""
    try:
        src = RemoteMySQLSource({
            "ssh_connection_info": SshConnectInfo(
                host=split_host_port(source)[0],
                user=cfg.get('ssh', 'ssh_user'),
                key=cfg.get('ssh', 'ssh_key')
            ),
            "mysql_connect_info": MySQLConnectInfo(
                cfg.get('mysql', 'mysql_defaults_file'),
                hostname=split_host_port(source)[0]
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

        datadir = src.datadir

        if dst.list_files(datadir):
            LOG.error("Destination datadir is not empty: %s", datadir)
            exit(1)
        try:
            dst.execute_command("service mysqld stop")
        except SshClientException:
            dst.execute_command("service mysql stop")

        proc_netcat = Process(
            target=dst.netcat,
            args=(
                "gunzip -c - | xbstream -x -C {datadir}".format(
                    datadir=datadir
                ),
            ),
            kwargs={
                'port': netcat_port
            }
        )
        proc_netcat.start()
        src.clone(
            dest_host=split_host_port(destination)[0],
            port=netcat_port
        )
        proc_netcat.join()
        src.clone_config(dst)

        dst_mysql = RemoteMySQLSource({
            "ssh_connection_info": SshConnectInfo(
                host=split_host_port(destination)[0],
                user=cfg.get('ssh', 'ssh_user'),
                key=cfg.get('ssh', 'ssh_key')
            ),
            "mysql_connect_info": MySQLConnectInfo(
                cfg.get('mysql', 'mysql_defaults_file'),
                hostname=split_host_port(destination)[0]
            ),
            "run_type": INTERVALS[0],
            "full_backup": INTERVALS[0],
        })

        dst_mysql.apply_backup(datadir)
        try:
            dst.execute_command("service mysqld start")
        except SshClientException:
            dst.execute_command("service mysql start")
        dst_mysql.setup_slave(source)

    except (ConfigParser.NoOptionError, OperationalError) as err:
        LOG.error(err)
        exit(1)
