# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up remote MySQL.
"""
import tempfile
import os
from contextlib import contextmanager

from spur import SshShell, NoSuchCommandError, CouldNotChangeDirectoryError
from spur.ssh import MissingHostKey
from twindb_backup import LOG
from twindb_backup.source.mysql_source import MySQLSource


class RemoteMySQLSource(MySQLSource):
    """Remote MySQLSource class"""

    def __init__(self, ssh_connection_info,
                 mysql_connect_info, run_type, full_backup, dst):

        self.ssh_shell = SshShell(hostname=ssh_connection_info.host,
                                  username=ssh_connection_info.user,
                                  port=ssh_connection_info.port,
                                  private_key_file=ssh_connection_info.key,
                                  missing_host_key=MissingHostKey.accept)

        super(RemoteMySQLSource, self).__init__(mysql_connect_info,
                                                run_type, full_backup,
                                                dst)

    @contextmanager
    def get_stream(self):
        """Get a PIPE handler with content of the source from remote server"""

        cmd = self._prepare_stream_cmd()
        stderr_file = tempfile.NamedTemporaryFile(delete=False)

        try:
            result = self.ssh_shell.run(cmd, stderr=stderr_file)
            yield result.output
            self._update_backup_info(stderr_file)
            os.unlink(stderr_file.name)
        except (NoSuchCommandError, CouldNotChangeDirectoryError) as err:
            LOG.error(err)
            LOG.error('Failed to run innobackupex. '
                      'Check error output in %s', stderr_file.name)
            self.dst.delete(self.get_name())
            exit(1)

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
