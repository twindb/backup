# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""
import ConfigParser
from multiprocessing import Process

from pymysql import OperationalError

from twindb_backup import INTERVALS, LOG, TwinDBBackupError
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.ssh.exceptions import SshClientException
from twindb_backup.util import split_host_port


def _mysql_service(dst, action):
    """Start or stop MySQL service

    :param dst: Destination server
    :type dst: Ssh
    :param action: string start or stop
    :type action: str
    """
    for service in ['mysqld', 'mysql']:
        try:
            return dst.execute_command(
                "service %s %s" % (service, action),
                quiet=True
            )
        except SshClientException:
            pass

    raise TwinDBBackupError('Failed to %s MySQL on %r'
                            % (action, dst))


def clone_mysql(cfg, source, destination,  # pylint: disable=too-many-arguments
                replication_user, replication_password,
                netcat_port=9990):
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
            _mysql_service(dst, action='stop')
        except TwinDBBackupError as err:
            LOG.error(err)
            exit(1)

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

        binlog, position = dst_mysql.apply_backup(datadir)

        try:
            _mysql_service(dst, action='start')
        except TwinDBBackupError as err:
            LOG.error(err)
            exit(1)

        dst_mysql.setup_slave(source,
                              replication_user, replication_password,
                              binlog, position)

    except (ConfigParser.NoOptionError, OperationalError) as err:
        LOG.error(err)
        exit(1)
