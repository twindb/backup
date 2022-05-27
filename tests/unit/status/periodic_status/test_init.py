import mock

from twindb_backup.status.periodic_status import PeriodicStatus


@mock.patch.object(PeriodicStatus, "_load")
def test_init_empty(mock_load):
    PeriodicStatus()
    mock_load.assert_not_called()


@mock.patch.object(PeriodicStatus, "_load")
def test_init(mock_load, status_raw_content):
    PeriodicStatus(content=status_raw_content)
    mock_load.assert_called_once_with(
        "{\r\n"
        '    "hourly": {\r\n'
        '      "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {\r\n'
        '        "backup_finished": 1522210295,\r\n'
        '        "binlog": "mysql-bin.000001",\r\n'
        '        "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",\r\n'
        '        "lsn": 19903207,\r\n'
        '        "galera": false,\r\n'
        '        "config": [\r\n'
        "          {\r\n"
        '            "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YXIvcnVuL215c3FsZC9teXNxbGQucGlkCg=="\r\n'
        "          }\r\n"
        "        ],\r\n"
        '        "backup_started": 1522210276,\r\n'
        '        "position": 46855,\r\n'
        '        "type": "incremental",\r\n'
        '        "wsrep_provider_version": null\r\n'
        "      }\r\n"
        "    },\r\n"
        '    "daily": {},\r\n'
        '    "weekly": {},\r\n'
        '    "monthly": {},\r\n'
        '    "yearly": {}\r\n'
        "}"
    )
