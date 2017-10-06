# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""
import ConfigParser

from twindb_backup import INTERVALS
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from multiprocessing import Process

from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


def run_netcat(dest_ssh):
    """Process handler"""
    dest_ssh.execute_command(['nc -l 9999'])


def get_ssh_credentials(cfg):
    """
    Get ssh credentials from config file
    :param cfg: Config file
    :type cfg: ConfigParser.ConfigParser
    :return: Ssh user and private_key
    :rtype: tuple
    """
    try:
        ssh_key = cfg.get('ssh', 'ssh_key')
    except ConfigParser.NoOptionError:
        ssh_key = '/root/.ssh/id_rsa'
    user = cfg.get('ssh', 'ssh_user')
    return user, ssh_key


def clone_mysql(cfg, dest_host, source_host, server_id, binary_logging): # pylint: disable=unused-argument
    """Clone mysql backup of remote machine and stream it to slave"""

    user, ssh_key = get_ssh_credentials(cfg)
    shell_info = SshConnectInfo(host=dest_host,
                                user=user,
                                key=ssh_key)
    dest_ssh = Ssh(shell_info, '/var/lib/mysql')
    proc_netcat = Process(target=run_netcat, args=(dest_ssh,))
    proc_netcat.start()
    shell_info.host=source_host
    source_mysql = RemoteMySQLSource(
        {
            "ssh_connection_info": shell_info,
            "mysql_connect_info": MySQLConnectInfo("empty_file"),
            "run_type": INTERVALS[0],
            "full_backup": INTERVALS[0],
            "dst": None
         })
    source_mysql.send_backup(dest_host)
    proc_netcat.join()




