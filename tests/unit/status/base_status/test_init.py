import mock
import pytest

from twindb_backup.status.base_status import BaseStatus
from twindb_backup.status.exceptions import CorruptedStatus


def test_init_none():
    status = BaseStatus()
    assert type(status) == BaseStatus
    assert len(status) == 0


def test_init_empty_raises():
    with pytest.raises(CorruptedStatus):
        assert BaseStatus(content="")


@mock.patch.object(BaseStatus, '_load')
def test_init_unpacks(mock_load, status_raw_content):
    status = BaseStatus(content=status_raw_content)
    assert status.version == 1
    mock_load.assert_called_once_with(
        '{\r\n'
        '    "hourly": {\r\n'
        '      "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {\r\n'
        '        "backup_finished": 1522210295,\r\n'
        '        "binlog": "mysql-bin.000001",\r\n'
        '        "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",\r\n'
        '        "lsn": 19903207,\r\n'
        '        "galera": false,\r\n'
        '        "config": [\r\n'
        '          {\r\n'
        '            "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YXIvcnVuL215c3FsZC9teXNxbGQucGlkCg=="\r\n'
        '          }\r\n'
        '        ],\r\n'
        '        "backup_started": 1522210276,\r\n'
        '        "position": 46855,\r\n'
        '        "type": "incremental",\r\n'
        '        "wsrep_provider_version": null\r\n'
        '      }\r\n'
        '    },\r\n'
        '    "daily": {},\r\n'
        '    "weekly": {},\r\n'
        '    "monthly": {},\r\n'
        '    "yearly": {}\r\n'
        '}')


def test_invalid_content_raises(status_raw_content_with_invalid_hash):
    with pytest.raises(CorruptedStatus):
        BaseStatus(content=status_raw_content_with_invalid_hash)


@mock.patch.object(BaseStatus, '_load')
def test_init_reads_deprecated(mock_load, deprecated_status_raw_content):
    status = BaseStatus(content=deprecated_status_raw_content)
    assert status.version == 1
    mock_load.assert_called_once_with(
        '        {\r\n'
        '              "monthly": {},\r\n'
        '              "hourly": {\r\n'
        '                "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {\r\n'
        '                  "backup_finished": 1522210295,\r\n'
        '                  "binlog": "mysql-bin.000001",\r\n'
        '                  "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",\r\n'
        '                  "lsn": 19903207,\r\n'
        '                  "galera": false,\r\n'
        '                  "config": [\r\n'
        '                    {\r\n'
        '                      "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YXIvcnVuL215c3FsZC9teXNxbGQucGlkCg=="\r\n'
        '                    }\r\n'
        '                  ],\r\n'
        '                  "backup_started": 1522210276,\r\n'
        '                  "position": 46855,\r\n'
        '                  "type": "incremental",\r\n'
        '                  "wsrep_provider_version": null\r\n'
        '                }\r\n'
        '              },\r\n'
        '              "daily": {\r\n'
        '                "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz": {\r\n'
        '                  "backup_finished": 1522210200,\r\n'
        '                  "binlog": "mysql-bin.000001",\r\n'
        '                  "lsn": 19903199,\r\n'
        '                  "parent": null,\r\n'
        '                  "type": "full",\r\n'
        '                  "wsrep_provider_version": null,\r\n'
        '                  "backup_started": 1522210193,\r\n'
        '                  "galera": false,\r\n'
        '                  "position": 46855,\r\n'
        '                  "config": [\r\n'
        '                    {\r\n'
        '                      "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YXIvcnVuL215c3FsZC9teXNxbGQucGlkCg=="\r\n'
        '                    }\r\n'
        '                  ]\r\n'
        '                }\r\n'
        '              },\r\n'
        '              "yearly": {},\r\n'
        '              "weekly": {}\r\n'
        '            }'
    )


@mock.patch.object(BaseStatus, '_load')
def test_init_reads_deprecated_invalid(
        mock_load,
        invalid_deprecated_status_raw_content):

    BaseStatus(content=invalid_deprecated_status_raw_content)
    mock_load.assert_called_once_with(
        '        {\r\n'
        '              "monthly": {},\r\n'
        '              "hourly": {\r\n'
        '                "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {\r\n'
        '                  "backup_finished": 1522210295,\r\n'
        '                  "binlog": "mysql-bin.000001",\r\n'
        '                  "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",\r\n'
        '                  "lsn": 19903207,\r\n'
        '                  "galera": false,\r\n'
        '                  "config": [\r\n'
        '                    {\r\n'
        '                      "/etc/my.cnf": "Foo-bar-bah"\r\n'
        '                    }\r\n'
        '                  ],\r\n'
        '            }'
    )
