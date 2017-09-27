from twindb_backup.source.mysql_source import MySQLSource


class RemoteMySQLSource(MySQLSource):

    """"""
    def __init__(self, ssh_connection_info,
                 mysql_connect_info, run_type, full_backup, dst):
        super(RemoteMySQLSource, self).__init__(mysql_connect_info,
                                                run_type, full_backup,
                                                dst)

