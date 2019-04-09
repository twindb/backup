from twindb_backup.status.mysql_status import MySQLStatus


def test_write_status(gs):
    status = MySQLStatus()
    status.save(gs)
