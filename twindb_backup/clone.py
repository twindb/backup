# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""
import ConfigParser
from multiprocessing import Process

from pymysql import OperationalError

from twindb_backup import INTERVALS, LOG, TwinDBBackupError, \
    MY_CNF_COMMON_PATHS
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.util import split_host_port


def get_root_my_cnf(src):
    """
    Return path on source server to root my.cnf

    :param src: Source server
    :type src: Ssh
    :return: Path to my.cnf
    :raise IOError: if my.cnf is not found
    """
    for mysql_config in MY_CNF_COMMON_PATHS:
        try:
            with src.get_stream(mysql_config):
                return mysql_config
        except SshDestinationError:
            continue
    raise IOError("Root my.cnf not found")


def get_remote_cfg(src):
    """
    Return parsed config from remote server
    :param src: Source server
    :return: Config
    :rtype: ConfigParser.ConfigParser
    """
    cfg_path = get_root_my_cnf(src)
    cfg = ConfigParser.ConfigParser(allow_no_value=True)
    try:
        with src.get_stream(cfg_path) as cfg_fp:
            cfg.readfp(cfg_fp)
    except ConfigParser.ParsingError as err:
        LOG.error(err)
        exit(1)
    return cfg


def save_cfg(src, dst, path):
    """
    Save configs from source to destination server

    :param src: Source server
    :param dst: Destination server
    :param path: Path to file
    """
    cfg = get_remote_cfg(src)
    cfg.set('mysqld', 'server_id', value=src.ssh_connect_info.host)
    for option in cfg.options('mysqld'):
        val = cfg.get('mysqld', option)
        if '!includedir' in option:
            for sub_file in src.list_files(prefix=val):
                save_cfg(src, dst, path + "/" + sub_file)
        elif '!include' in option:
            save_cfg(src, dst, val)
    with dst.get_remote_stdin(["cat - > ", path]) as cin:
        cfg.write(cin)


def apply_backup(dst, datadir):
    """
    Apply backup of destination server

    :param dst: Destination server
    :param datadir: Path to datadir
    :raise TwinDBBackupError: if binary positions is different
    """
    dst.execute_command(['sudo', 'innobackupex', '--apply-log', datadir])
    _, stdout_, _ = dst.execute_command(
        [
            'sudo',
            'cat',
            datadir + "/xtrabackup_binlog_pos_innodb"
        ]
    )
    binlog_pos = stdout_.read().strip()
    _, stdout_, _ = dst.execute_command(
        [
            'sudo',
            'cat',
            datadir + "/xtrabackup_binlog_info"
        ]
    )
    binlog_info = stdout_.read().strip()
    if binlog_pos in binlog_info:
        return
    raise TwinDBBackupError("Invalid backup")


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

        # save_cfg(src_ssh, dst, get_root_my_cnf(src_ssh))
        # apply_backup(dst, src.datadir)
        # # Implement CHANGE MASTER TO
    except (ConfigParser.NoOptionError, OperationalError) as err:
        LOG.error(err)
        exit(1)

