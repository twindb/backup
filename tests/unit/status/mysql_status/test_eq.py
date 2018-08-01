from twindb_backup.status.mysql_status import MySQLStatus


def test_eq(deprecated_status_raw_content):
    status_1 = MySQLStatus(content=deprecated_status_raw_content)
    status_2 = MySQLStatus(content=deprecated_status_raw_content)
    assert status_1 == status_2
