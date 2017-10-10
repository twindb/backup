# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import socket

from contextlib import contextmanager

import time
from paramiko import SSHClient, AutoAddPolicy, SSHException, \
    AuthenticationException

from twindb_backup import LOG
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
