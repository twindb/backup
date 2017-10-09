# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""

from multiprocessing import Process
import ConfigParser

from twindb_backup import INTERVALS, LOG
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.util import split_host_port


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
    src.clone()

    # user, ssh_key = get_ssh_credentials(cfg)
    # shell_info = SshConnectInfo(host=dest_host,
    #                             user=user,
    #                             key=ssh_key)
    # dest_ssh = Ssh(shell_info, '/var/lib/mysql')
    # proc_netcat = Process(target=run_netcat, args=(dest_ssh,))
    # proc_netcat.start()
    # shell_info.host = source_host
    # source_mysql = RemoteMySQLSource(
    #     {
    #         "ssh_connection_info": shell_info,
    #         "mysql_connect_info": MySQLConnectInfo("empty_file"),
    #         "run_type": INTERVALS[0],
    #         "full_backup": INTERVALS[0],
    #         "dst": None
    #     }
    # )
    # source_mysql.send_backup(dest_host)
    # proc_netcat.join()
