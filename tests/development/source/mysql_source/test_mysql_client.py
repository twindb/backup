from os.path import expanduser

from twindb_backup.source.mysql_source import MySQLClient


def test_client():
    client = MySQLClient(defaults_file=expanduser("~/.my.cnf"))
    print(client.server_vendor)
    print(client.variable("log_bin_basename"))
