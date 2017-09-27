import socket
from contextlib import contextmanager

import paramiko
from paramiko import SSHException, BadHostKeyException, AuthenticationException

from twindb_backup import LOG
from twindb_backup.source.mysql_source import MySQLSource


class RemoteMySQLSource(MySQLSource):
    """Remote MySQLSource class"""

    def __init__(self, ssh_connection_info,
                 mysql_connect_info, run_type, full_backup, dst):
        self.ssh_connection_info = ssh_connection_info
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        super(RemoteMySQLSource, self).__init__(mysql_connect_info,
                                                run_type, full_backup,
                                                dst)

    @contextmanager
    def get_stream(self):
        """Get a PIPE handler with content of the source from remote server"""
        cmd = self._prepare_stream_cmd()
        try:
            self.ssh_client.connect(self.ssh_connection_info.host,
                                    self.ssh_connection_info.port,
                                    username=self.ssh_connection_info.user,
                                    key_filename=self.ssh_connection_info.key)
            stdin, stdout, stderr = self.ssh_client.exec_command(" ".join(cmd))
        except (SSHException, AuthenticationException,
                BadHostKeyException, socket.error) as err:
            LOG.error(err)
        finally:
            self.ssh_client.close()
