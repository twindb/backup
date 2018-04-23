from twindb_backup.status.mysql_status import MySQLStatus


def test_backup_duration(status_raw_content):
    status = MySQLStatus(status_raw_content)
    assert status.backup_duration(
        'hourly',
        'master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz'
    ) == 19
