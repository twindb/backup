# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import tempfile
import os
from contextlib import contextmanager

from paramiko import SSHClient, AutoAddPolicy, SSHException

from twindb_backup.source.mysql_source import MySQLSource


class RemoteMySQLSource(MySQLSource):
    """Remote MySQLSource class"""

    def __init__(self, kwargs):

        self.ssh_connection_info = kwargs['ssh_connection_info']
        super(RemoteMySQLSource, self).__init__(**kwargs)

    @contextmanager
    def get_stream(self):
        """Get a PIPE handler with content of the source from remote server"""

        cmd = self._prepare_stream_cmd()
        stderr_file = tempfile.NamedTemporaryFile(delete=False)
        shell = SSHClient()
        shell.connect(hostname=self.ssh_connection_info.host,
                                  username=self.ssh_connection_info.user,
                                  port=self.ssh_connection_info.port,
                                  key_filename=self.ssh_connection_info.key)
        shell.set_missing_host_key_policy(AutoAddPolicy())
        try:
            _, stdout, stderr = shell.exec_command(cmd)
            yield stdout
            self._update_backup_info(stderr_file)
            os.unlink(stderr_file.name)
        except SSHException as err:
            self._handle_failure_exec(err, stderr_file)

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
