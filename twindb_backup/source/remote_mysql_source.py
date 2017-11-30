# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import ConfigParser
import socket
import struct

from contextlib import contextmanager

import time

import pymysql
from twindb_backup import LOG, MY_CNF_COMMON_PATHS
from twindb_backup.destination.exceptions import SshDestinationError
from twindb_backup.source.exceptions import RemoteMySQLSourceError
from twindb_backup.source.mysql_source import MySQLSource
from twindb_backup.ssh.client import SshClient
from twindb_backup.ssh.exceptions import SshClientException


class RemoteMySQLSource(MySQLSource):
    """Remote MySQLSource class"""

    def __init__(self, kwargs):
        self._ssh_client = SshClient(kwargs.pop('ssh_connection_info'))
        super(RemoteMySQLSource, self).__init__(**kwargs)

    @contextmanager
    def get_stream(self):
        raise NotImplementedError("Method get_stream not implemented")

    def clone(self, dest_host, port):
        """
        Send backup to destination host

        :param dest_host: Destination host
        :type dest_host: str
        :param port: Port to sending backup
        :type port: int
        :raise RemoteMySQLSourceError: if any error
        """
        retry = 1
        retry_time = 2
        cmd = "bash -c \"sudo innobackupex --stream=xbstream ./ " \
              "| nc %s %d\"" \
              % (dest_host, port)
        while retry < 3:
            try:
                return self._ssh_client.execute(cmd)
            except SshClientException as err:
                LOG.warning(err)
                LOG.info('Will try again in after %d seconds', retry_time)
                time.sleep(retry_time)
                retry_time *= 2

    def clone_config(self, dst):
        """
        Clone config to destination server

        :param dst: Destination server
        :type dst: Ssh
        """
        cfg_path = self._get_root_my_cnf()
        self._save_cfg(dst, cfg_path)

    def _save_cfg(self, dst, path):
        """Save configs on destination recursively"""
        cfg = self._get_config(path)
        server_id = self._get_server_id(dst.host)
        cfg.set('mysqld', 'server_id', value=str(server_id))
        for option in cfg.options('mysqld'):
            val = cfg.get('mysqld', option)
            if '!includedir' in option:
                val = val.split()[1]
                ls_cmd = 'ls %s' % val
                with self._ssh_client.get_remote_handlers(ls_cmd) \
                        as (_, cout, _):
                    file_list = sorted(cout.read().split())
                for sub_file in file_list:
                    self._save_cfg(dst, val + "/" + sub_file)
            elif '!include' in option:
                self._save_cfg(dst, val.split()[1])

        with dst.client.get_remote_handlers("cat - > %s" % path) \
                as (cin, _, _):
            cfg.write(cin)

    def _get_root_my_cnf(self):
        """Return root my.cnf path"""
        for cfg_path in MY_CNF_COMMON_PATHS:
            try:
                cmd = "cat %s" % cfg_path
                with self._ssh_client.get_remote_handlers(cmd):
                    return cfg_path
            except SshDestinationError:
                continue
        raise OSError("Root my.cnf not found")

    def _get_config(self, cfg_path):
        """
        Return parsed config

        :param cfg_path: Path to config
        :type cfg_path: str
        :return: Path and config
        :rtype: ConfigParser.ConfigParser
        """
        cfg = ConfigParser.ConfigParser(allow_no_value=True)
        try:
            cmd = "cat %s" % cfg_path
            with self._ssh_client.get_remote_handlers(cmd) as (_, cout, _):
                cfg.readfp(cout)
        except ConfigParser.ParsingError as err:
            LOG.error(err)
            exit(1)
        return cfg

    def setup_slave(self, host, user, password, binlog, binlog_position):  # noqa # pylint: disable=too-many-arguments
        """
        Change master

        :param host: Master host name.
        :type host: str
        :param user: Replication user.
        :param password: Replication password
        :param binlog: Binlog file on the master
        :param binlog_position: Binlog position

        """
        try:
            with self._cursor() as cursor:
                query = "CHANGE MASTER TO " \
                        "MASTER_HOST = '{master}', " \
                        "MASTER_USER = '{user}', " \
                        "MASTER_PASSWORD = '{password}', " \
                        "MASTER_LOG_FILE = '{binlog}', " \
                        "MASTER_LOG_POS = {binlog_pos}"\
                    .format(
                        master=host,
                        user=user,
                        password=password,
                        binlog=binlog,
                        binlog_pos=binlog_position)
                cursor.execute(query)
                cursor.execute("START SLAVE")
            return True
        except pymysql.Error as err:
            LOG.debug(err)
            return False

    def apply_backup(self, datadir):
        """
        Apply backup of destination server

        :param datadir: Path to datadir
        :return: Binlog file name and position
        :rtype: tuple
        :raise TwinDBBackupError: if binary positions is different
        """

        try:
            self._ssh_client.execute(
                'sudo innobackupex --apply-log %s --use-memory %d'
                % (datadir, self._mem_available() / 2)
            )
        except OSError:
            self._ssh_client.execute(
                'sudo innobackupex --apply-log %s' % datadir
            )

        self._ssh_client.execute("sudo chown -R mysql %s" % datadir)

        _, stdout_, _ = self._ssh_client.execute(
            'sudo cat %s/xtrabackup_binlog_pos_innodb' % datadir
        )
        binlog_pos = stdout_.read().strip()
        _, stdout_, _ = self._ssh_client.execute(
            'sudo cat %s/xtrabackup_binlog_info' % datadir
        )
        binlog_info = stdout_.read().strip()
        if binlog_pos in binlog_info:
            return tuple(binlog_info.split())
        raise RemoteMySQLSourceError("Invalid backup")

    def _mem_available(self):
        """
        Get available memory size

        :return: Size of available memory in bytes
        :raise OSError: if can' detect memory
        """
        # https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0a
        _, stdout_, _ = self._ssh_client.execute(
            "awk -v low=$(grep low /proc/zoneinfo | "
            "awk '{k+=$2}END{print k}') "
            "'{a[$1]=$2}END{m="
            "a[\"MemFree:\"]"
            "+a[\"Active(file):\"]"
            "+a[\"Inactiv‌​e(file):\"]"
            "+a[\"SRecla‌​imable:\"]; "
            "print a[\"MemAvailable:\"]}' "
            "/proc/meminfo"
        )
        mem = stdout_.read().strip()
        if not mem:
            raise OSError("Cant get available mem")
        free_mem = int(mem) * 1024
        return free_mem

    @staticmethod
    def _get_server_id(host):
        """Determinate server id"""
        try:
            server_id = struct.unpack("!I", socket.inet_aton(host))[0]
        except socket.error:
            server_ip = socket.gethostbyname(host)
            server_id = struct.unpack("!I", socket.inet_aton(server_ip))[0]
        return server_id
