from twindb_backup.status.mysql_status import MySQLStatus


def test_eq_empty():
    status1 = MySQLStatus()
    status2 = MySQLStatus()
    assert status1 == status2
    assert status1.valid
    assert status2.valid
