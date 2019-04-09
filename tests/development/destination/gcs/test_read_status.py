from twindb_backup.status.mysql_status import MySQLStatus


def test_read_status(gs):
    print(MySQLStatus(dst=gs))

