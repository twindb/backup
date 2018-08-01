from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.mysql_status import MySQLStatus


def test_get_item_returns_copy_by_basename(deprecated_status_raw_content):
    status = MySQLStatus(deprecated_status_raw_content)
    key = "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz"
    copy = status[key]
    assert type(copy) == MySQLCopy
    assert copy.run_type == 'hourly'
    assert copy.host == 'master1'
    assert copy.name == 'mysql-2018-03-28_04_11_16.xbstream.gz'
