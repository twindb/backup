# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import ConfigParser
import socket
import struct
import re
from contextlib import contextmanager

import time
from errno import ENOENT

import pymysql
from twindb_backup import LOG, MY_CNF_COMMON_PATHS
from twindb_backup.source.exceptions import RemoteMySQLSourceError
from twindb_backup.source.mysql_source import MySQLSource
from twindb_backup.ssh.client import SshClient
from twindb_backup.ssh.exceptions import SshClientException


class RemoteMySQLSource(MySQLSource):
    """Remote MySQLSource class"""

    def __init__(self, kwargs):

        ssh_kwargs = {}
        for arg in ['ssh_host', 'ssh_port', 'ssh_user', 'ssh_key']:
            if arg in kwargs:
                ssh_kwargs[arg.replace('ssh_', '')] = kwargs.pop(arg)

        self._ssh_client = SshClient(**ssh_kwargs)

        super(RemoteMySQLSource, self).__init__(**kwargs)

    @contextmanager
    def get_stream(self):
        raise NotImplementedError("Method get_stream not implemented")

    def clone(self, dest_host, port, compress=False):
        """
        Send backup to destination host

        :param dest_host: Destination host
        :type dest_host: str
        :param port: Port to sending backup
        :type port: int
        :param compress: If True compress stream
        :type compress: bool
        :raise RemoteMySQLSourceError: if any error
        """
        retry = 1
        retry_time = 2
        error_log = "/tmp/{src}_{src_port}-{dst}_{dst_port}.log".format(
            src=self._ssh_client.host,
            src_port=self._ssh_client.port,
            dst=dest_host,
            dst_port=port
        )
        if compress:
            compress_cmd = "| gzip -c - "
        else:
            compress_cmd = ""

        cmd = "bash -c \"sudo %s " \
              "--stream=xbstream " \
              "--host=127.0.0.1 " \
              "--backup " \
              "--target-dir ./ 2> %s" \
              " %s | ncat %s %d --send-only\"" \
              % (self._xtrabackup, error_log, compress_cmd, dest_host, port)
        while retry < 3:
            try:
                return self._ssh_client.execute(cmd)
            except SshClientException as err:
                LOG.warning(err)
                LOG.info('Will try again in after %d seconds', retry_time)
                time.sleep(retry_time)
                retry_time *= 2
                retry += 1

    def clone_config(self, dst):
        """
        Clone config to destination server

        :param dst: Destination server
        :type dst: Ssh
        """
        cfg_path = self._get_root_my_cnf()
        LOG.debug("Root my.cnf is: %s", cfg_path)
        self._save_cfg(dst, cfg_path)

    def _find_all_cnf(self, root_path):
        """ Return list of embed cnf files"""
        files = [root_path]
        cfg_content = self._ssh_client.get_text_content(root_path)
        for line in cfg_content.splitlines():
            if '!includedir' in line:
                path = line.split()[1]
                file_list = self._ssh_client.list_files(
                    path,
                    recursive=False,
                    files_only=True
                )
                for sub_file in file_list:
                    files.extend(
                        self._find_all_cnf(
                            sub_file
                        )
                    )
            elif '!include' in line:
                files.extend(
                    self._find_all_cnf(
                        line.split()[1]
                    )
                )
        return files

    @staticmethod
    def _find_server_id_by_path(cfg):
        """Find path with server_id"""
        options = ["server_id", "server-id"]
        for option in options:
            try:
                if cfg.has_option("mysqld", option):
                    return option
            except ConfigParser.Error:
                pass
        return None

    def _save_cfg(self, dst, root_cfg):
        """Save configs on destination recursively"""
        files = self._find_all_cnf(root_cfg)
        server_id = self._get_server_id(dst.host)
        is_server_id_set = False
        valid_cfg = []
        for path in files:
            try:
                cfg = self._get_config(path)
                option = self._find_server_id_by_path(cfg)
                if option:
                    cfg.set('mysqld', option, value=str(server_id))
                    is_server_id_set = True
                dst.client.write_config(path, cfg)
                valid_cfg.append(path)
            except ConfigParser.ParsingError:
                cfg_content = self._ssh_client.get_text_content(path)
                dst.client.write_content(path, cfg_content)

        if not is_server_id_set:
            for path in valid_cfg:
                cfg = self._get_config(path)
                if cfg.has_section("mysqld"):
                    cfg.set('mysqld', "server_id", value=str(server_id))
                    dst.client.write_config(path, cfg)
                    return

    def _get_root_my_cnf(self):
        """Return root my.cnf path"""
        for cfg_path in MY_CNF_COMMON_PATHS:
            try:
                self._ssh_client.get_text_content(cfg_path)
                return cfg_path
            except SshClientException:
                continue
            except IOError as err:
                if err.errno == ENOENT:
                    continue
                else:
                    raise
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
            raise
        return cfg

    def setup_slave(self, master_info):  # noqa # pylint: disable=too-many-arguments
        """
        Change master

        :param master_info: Master details.
        :type master_info: MySQLMasterInfo

        """
        try:
            with self._cursor() as cursor:
                query = "CHANGE MASTER TO " \
                        "MASTER_HOST = '{master}', " \
                        "MASTER_USER = '{user}', " \
                        "MASTER_PORT = {port}, " \
                        "MASTER_PASSWORD = '{password}', " \
                        "MASTER_LOG_FILE = '{binlog}', " \
                        "MASTER_LOG_POS = {binlog_pos}"\
                    .format(
                        master=master_info.host,
                        user=master_info.user,
                        password=master_info.password,
                        binlog=master_info.binlog,
                        binlog_pos=master_info.binlog_position,
                        port=master_info.port
                    )
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
        :raise RemoteMySQLSourceError: if any error.
        """
        try:
            use_memory = "--use-memory %d" % int(self._mem_available() / 2)
        except OSError:
            use_memory = ""
        logfile_path = "/tmp/xtrabackup-apply-log.log"
        cmd = "sudo {xtrabackup} --prepare --apply-log-only " \
              "--target-dir {target_dir} {use_memory} " \
              "> {logfile} 2>&1" \
              "".format(
                  xtrabackup=self._xtrabackup,
                  target_dir=datadir,
                  use_memory=use_memory,
                  logfile=logfile_path
              )

        try:
            self._ssh_client.execute(cmd)
            self._ssh_client.execute("sudo chown -R mysql %s" % datadir)
            return self._get_binlog_info(datadir)
        except SshClientException as err:
            LOG.debug("Logfile is:")
            LOG.debug(self._ssh_client.get_text_content(logfile_path))
            raise RemoteMySQLSourceError(err)

    def _mem_available(self):
        """
        Get available memory size

        :return: Size of available memory in bytes
        :rtype: int
        :raise OSError: if can' detect memory
        """
        # https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0a
        stdout_, _ = self._ssh_client.execute(
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
        mem = stdout_.strip()
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

    def _get_binlog_info(self, backup_path):
        """Get binlog coordinates from an xtrabackup_binlog_info.

        :param backup_path: Path where to look for xtrabackup_binlog_info.
        :type backup_path: str
        :return: Tuple with binlog coordinates - (file_name, pos)
        :rtype: tuple
        """
        stdout_, _ = self._ssh_client.execute(
            'sudo cat %s/xtrabackup_binlog_info' % backup_path
        )
        binlog_info = re.split(r'\t+', stdout_.rstrip())
        return binlog_info[0], int(binlog_info[1])
