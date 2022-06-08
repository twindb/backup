import json

from twindb_backup.status.mysql_status import MySQLStatus


def test_str_old(deprecated_status_raw_content):
    status = MySQLStatus(deprecated_status_raw_content)
    expected = (
        "{"
        '"monthly": {}, '
        '"hourly": {'
        '"master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {'
        '"galera": false, '
        '"binlog": '
        '"mysql-bin.000001", '
        '"run_type": "hourly", '
        '"name": "mysql-2018-03-28_04_11_16.xbstream.gz", '
        '"parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz", '
        '"lsn": 19903207, '
        '"server_vendor": "oracle", '
        '"type": "incremental", '
        '"backup_finished": 1522210295, '
        '"wsrep_provider_version": null, '
        '"host": "master1", '
        '"backup_started": 1522210276, '
        '"position": 46855, '
        '"config": [{'
        '"/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhc'
        "i9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLW"
        "xpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3M"
        "Kc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1t"
        "eXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5Cgpbb"
        "XlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YX"
        'IvcnVuL215c3FsZC9teXNxbGQucGlkCg=="}]}}, '
        '"yearly": {}, '
        '"daily": {'
        '"master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz": {'
        '"galera": false, '
        '"binlog": "mysql-bin.000001", '
        '"run_type": "daily", '
        '"name": "mysql-2018-03-28_04_09_53.xbstream.gz", '
        '"parent": null, '
        '"lsn": 19903199, '
        '"server_vendor": "oracle", '
        '"type": "full", '
        '"backup_finished": 1522210200, '
        '"wsrep_provider_version": null, '
        '"host": "master1", '
        '"backup_started": 1522210193, '
        '"position": 46855, '
        '"config": [{'
        '"/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhc'
        "i9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLW"
        "xpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3M"
        "Kc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1t"
        "eXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5Cgpbb"
        "XlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YX"
        'IvcnVuL215c3FsZC9teXNxbGQucGlkCg=="}]}}, '
        '"weekly": {}}'
    )

    assert json.loads(str(status)) == json.loads(expected)


def test_str_new(status_raw_content):
    status = MySQLStatus(status_raw_content)
    expected = (
        '{"monthly": {}, '
        '"hourly": {'
        '"master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {'
        '"galera": false, '
        '"binlog": "mysql-bin.000001", '
        '"run_type": "hourly", '
        '"name": "mysql-2018-03-28_04_11_16.xbstream.gz", '
        '"parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz", '
        '"lsn": 19903207, '
        '"server_vendor": "oracle", '
        '"type": "incremental", '
        '"backup_finished": 1522210295, '
        '"wsrep_provider_version": null, '
        '"host": "master1", '
        '"backup_started": 1522210276, '
        '"position": 46855, "config": [{'
        '"/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhc'
        "i9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLW"
        "xpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3M"
        "Kc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1t"
        "eXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5Cgpbb"
        "XlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YX"
        'IvcnVuL215c3FsZC9teXNxbGQucGlkCg=="}]}}, '
        '"yearly": {}, '
        '"daily": {}, '
        '"weekly": {}}'
    )

    assert json.loads(str(status)) == json.loads(expected)
