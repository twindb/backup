from twindb_backup.status.mysql_status import MySQLStatus


def test_basename():
    assert MySQLStatus().basename == 'status'
