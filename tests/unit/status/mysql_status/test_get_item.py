import json
from base64 import b64decode

from twindb_backup.status.mysql_status import MySQLStatus


def test_get_item_returns_copy_by_basename(status_raw_content):
    status = MySQLStatus(status_raw_content)
    backup_copy = status["master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz"]
    decoded_status = json.loads(b64decode(status_raw_content))["hourly"]["master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz"]
    backup_copy_dict = backup_copy.as_dict()
    assert cmp(backup_copy_dict, decoded_status) == 0
