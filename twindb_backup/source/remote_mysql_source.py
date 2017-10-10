# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import ConfigParser
import socket

from contextlib import contextmanager

import time

import pymysql
from twindb_backup import LOG, MY_CNF_COMMON_PATHS
from twindb_backup.destination.exceptions import SshDestinationError
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
        cmd = "bash -c \"innobackupex --stream=xbstream ./ " \
              "| gzip -c - " \
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
        cfg.set('mysqld', 'server_id',
                value=self._ssh_client.ssh_connect_info.host)
        for option in cfg.options('mysqld'):
            val = cfg.get('mysqld', option)
            if '!includedir' in option:
                val = val.split()[1]
                ls_cmd = 'ls %s' % val
                with self._ssh_client.get_remote_handlers(ls_cmd) as (_, cout, _):
                    file_list = sorted(cout.read().split())
                for sub_file in file_list:
                    self._save_cfg(dst, val + "/" + sub_file)
            elif '!include' in option:
                self._save_cfg(dst, val.split()[1])

        with dst.client.get_remote_handlers("sudo cat - > %s" % path) as (cin, _, _):
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

    def change_master_host(self, host):
        """
        Change master
        :param host: Master hostame
        :type host: str
        """
        try:
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("CHANGE MASTER TO MASTER_HOST='%s'" % host)
            return True
        except pymysql.Error as err:
            LOG.debug(err)
            return False
