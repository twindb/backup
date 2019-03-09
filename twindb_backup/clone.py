# -*- coding: utf-8 -*-
"""
Module defines clone feature
"""
from multiprocessing import Process

import time

from twindb_backup import INTERVALS, LOG, TwinDBBackupError
from twindb_backup.destination.ssh import Ssh
from twindb_backup.source.mysql_source import MySQLConnectInfo, MySQLMasterInfo
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
                "PATH=$PATH:/sbin sudo service %s %s" % (service, action),
                quiet=True
            )
        except SshClientException as err:
            LOG.debug(err)

    try:
        LOG.warning('Failed to %s MySQL with an init script. '
                    'Will try to %s mysqld.', action, action)
        if action == "start":
            ret = dst.execute_command(
                "PATH=$PATH:/sbin sudo bash -c 'nohup mysqld &'",
                background=True
            )
            time.sleep(10)
            return ret
        elif action == "stop":
            return dst.execute_command(
                "PATH=$PATH:/sbin sudo kill $(pidof mysqld)"
            )
    except SshClientException as err:
        LOG.error(err)
        raise TwinDBBackupError('Failed to %s MySQL on %r'
                                % (action, dst))


def clone_mysql(cfg, source, destination,  # pylint: disable=too-many-arguments
                replication_user, replication_password,
                netcat_port=9990,
                compress=False):
    """Clone mysql backup of remote machine and stream it to slave

    :param cfg: TwinDB Backup tool config
    :type cfg: TwinDBBackupConfig
    """
    LOG.debug('Remote MySQL Source: %s', split_host_port(source)[0])
    LOG.debug(
        'MySQL defaults: %s',
        cfg.mysql.defaults_file
    )
    LOG.debug(
        'SSH username: %s',
        cfg.ssh.user
    )
    LOG.debug(
        'SSH key: %s',
        cfg.ssh.key
    )
    src = RemoteMySQLSource(
        {
            "ssh_host": split_host_port(source)[0],
            "ssh_user": cfg.ssh.user,
            "ssh_key": cfg.ssh.key,
            "mysql_connect_info": MySQLConnectInfo(
                cfg.mysql.defaults_file,
                hostname=split_host_port(source)[0]),
            "run_type": INTERVALS[0],
            "backup_type": 'full'
        }
    )
    xbstream_binary = cfg.mysql.xbstream_binary
    LOG.debug('SSH destination: %s', split_host_port(destination)[0])
    LOG.debug('SSH username: %s', cfg.ssh.user)
    LOG.debug('SSH key: %s', cfg.ssh.key)
    dst = Ssh(
        '/tmp',
        ssh_host=split_host_port(destination)[0],
        ssh_user=cfg.ssh.user,
        ssh_key=cfg.ssh.key
    )
    datadir = src.datadir
    LOG.debug('datadir: %s', datadir)

    if dst.list_files(datadir):
        LOG.error("Destination datadir is not empty: %s", datadir)
        exit(1)

    _run_remote_netcat(
        compress,
        datadir,
        destination,
        dst,
        netcat_port,
        src,
        xbstream_binary
    )
    LOG.debug('Copying MySQL config to the destination')
    src.clone_config(dst)

    LOG.debug('Remote MySQL destination: %s',
              split_host_port(destination)[0])
    LOG.debug(
        'MySQL defaults: %s',
        cfg.mysql.defaults_file
    )
    LOG.debug('SSH username: %s', cfg.ssh.user)
    LOG.debug('SSH key: %s', cfg.ssh.key)

    dst_mysql = RemoteMySQLSource({
        "ssh_host": split_host_port(destination)[0],
        "ssh_user": cfg.ssh.user,
        "ssh_key": cfg.ssh.key,
        "mysql_connect_info": MySQLConnectInfo(
            cfg.mysql.defaults_file,
            hostname=split_host_port(destination)[0]
        ),
        "run_type": INTERVALS[0],
        "backup_type": 'full'
    })

    binlog, position = dst_mysql.apply_backup(datadir)

    LOG.debug('Binlog coordinates: (%s, %d)', binlog, position)

    try:
        LOG.debug('Starting MySQL on the destination')
        _mysql_service(dst, action='start')
        LOG.debug('MySQL started')
    except TwinDBBackupError as err:
        LOG.error(err)
        exit(1)

    LOG.debug('Setting up replication.')
    LOG.debug('Master host: %s', source)
    LOG.debug('Replication user: %s', replication_user)
    LOG.debug('Replication password: %s', replication_password)
    dst_mysql.setup_slave(
        MySQLMasterInfo(
            host=split_host_port(source)[0],
            port=split_host_port(source)[1],
            user=replication_user,
            password=replication_password,
            binlog=binlog,
            binlog_pos=position
        )
    )


def _run_remote_netcat(compress, datadir,  # pylint: disable=too-many-arguments
                       destination, dst, netcat_port, src, xbstream_path):
    netcat_cmd = "{xbstream_binary} -x -C {datadir}".format(
        xbstream_binary=xbstream_path,
        datadir=datadir
    )
    if compress:
        netcat_cmd = "gunzip -c - | %s" % netcat_cmd

    # find unused port
    while netcat_port < 64000:
        if dst.ensure_tcp_port_listening(netcat_port, wait_timeout=1):
            netcat_port += 1
        else:
            LOG.debug('Will use port %d for streaming', netcat_port)
            break
    proc_netcat = Process(
        target=dst.netcat,
        args=(netcat_cmd,),
        kwargs={
            'port': netcat_port
        }
    )
    LOG.debug('Starting netcat on the destination')
    proc_netcat.start()
    nc_wait_timeout = 10
    if not dst.ensure_tcp_port_listening(netcat_port,
                                         wait_timeout=nc_wait_timeout):
        LOG.error('netcat on the destination '
                  'is not ready after %d seconds', nc_wait_timeout)
        proc_netcat.terminate()
        exit(1)
    src.clone(
        dest_host=split_host_port(destination)[0],
        port=netcat_port,
        compress=compress
    )
    proc_netcat.join()
