# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import socket

from contextlib import contextmanager

from paramiko import SSHClient, AutoAddPolicy, SSHException, AuthenticationException

from twindb_backup import LOG
from twindb_backup.source.mysql_source import MySQLSource


class RemoteMySQLSourceError(Exception):
    """Errors during backups"""


class RemoteMySQLSource(MySQLSource):
    """Remote MySQLSource class"""

    def __init__(self, kwargs):
        self.ssh_connection_info = kwargs['ssh_connection_info']
        del kwargs['ssh_connection_info']
        super(RemoteMySQLSource, self).__init__(**kwargs)

    @contextmanager
    def get_stream(self):
        raise NotImplementedError("Method get_stream not implemented")

    def enable_wsrep_desync(self):
        raise NotImplementedError("Method enable_wsrep_desync not implemented")

    def disable_wsrep_desync(self):
        raise NotImplementedError("Method disable_wsrep_desync not implemented")

    def wsrep_provider_version(self):
        raise NotImplementedError("Method wsrep_provider_version not implemented")

    def is_galera(self):
        raise NotImplementedError("Method is_galera not implemented")

    def get_connection(self):
        raise NotImplementedError("Method get_connection not implemented")

    @property
    def galera(self):
        raise NotImplementedError("Property galera not implemented")

    def clone(self, dest_host, port):
        """
        Send backup to destination host

        :param dest_host: Destination host
        :type dest_host: str
        :param port: Port to sending backup
        :type port: int
        :raise RemoteMySQLSourceError: if any error
        """
        cmd = "bash -c \"innobackupex --stream=xbstream ./ " \
              "| gzip -c - " \
              "| nc %s %d\"" \
              % (dest_host, port)
        shell = SSHClient()
        shell.set_missing_host_key_policy(AutoAddPolicy())
        try:
            shell.connect(hostname=self.ssh_connection_info.host,
                          key_filename=self.ssh_connection_info.key,
                          port=self.ssh_connection_info.port,
                          username=self.ssh_connection_info.user)
            _, stdout_, stderr_ = shell.exec_command(cmd)
            if stdout_.channel.recv_exit_status() != 0:
                LOG.error("Failed while send_backup: %s", stderr_.read())
                raise RemoteMySQLSourceError('%s exited with code' % cmd)
        except (AuthenticationException, SSHException, socket.error) as err:
            raise RemoteMySQLSourceError(err)
        finally:
            shell.close()
