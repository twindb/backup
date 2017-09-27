import paramiko

from twindb_backup.source.mysql_source import MySQLSource


class RemoteMySQLSource(MySQLSource):

    """"""
    def __init__(self, ssh_connection_info,
                 mysql_connect_info, run_type, full_backup, dst):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        super(RemoteMySQLSource, self).__init__(mysql_connect_info,
                                                run_type, full_backup,
                                                dst)

