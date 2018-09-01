import pytest

from twindb_backup import setup_logging, LOG

setup_logging(LOG, debug=True)



@pytest.fixture
def status_raw_content():
    """
    Return raw content of status
    Status is:
{
    "hourly": {
      "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {
        "backup_finished": 1522210295,
        "binlog": "mysql-bin.000001",
        "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",
        "lsn": 19903207,
        "galera": false,
        "config": [
          {
            "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YXIvcnVuL215c3FsZC9teXNxbGQucGlkCg=="
          }
        ],
        "backup_started": 1522210276,
        "position": 46855,
        "type": "incremental",
        "wsrep_provider_version": null
      }
    },
    "daily": {},
    "weekly": {},
    "monthly": {},
    "yearly": {}
}
    :return:
    """
    return """
    {
  "status": "ew0KICAgICJob3VybHkiOiB7DQogICAgICAibWFzdGVyMS9ob3VybHkvbXlzcWwvbXlzcWwtMjAxOC0wMy0yOF8wNF8xMV8xNi54YnN0cmVhbS5neiI6IHsNCiAgICAgICAgImJhY2t1cF9maW5pc2hlZCI6IDE1MjIyMTAyOTUsDQogICAgICAgICJiaW5sb2ciOiAibXlzcWwtYmluLjAwMDAwMSIsDQogICAgICAgICJwYXJlbnQiOiAibWFzdGVyMS9kYWlseS9teXNxbC9teXNxbC0yMDE4LTAzLTI4XzA0XzA5XzUzLnhic3RyZWFtLmd6IiwNCiAgICAgICAgImxzbiI6IDE5OTAzMjA3LA0KICAgICAgICAiZ2FsZXJhIjogZmFsc2UsDQogICAgICAgICJjb25maWciOiBbDQogICAgICAgICAgew0KICAgICAgICAgICAgIi9ldGMvbXkuY25mIjogIlcyMTVjM0ZzWkYwS1pHRjBZV1JwY2owdmRtRnlMMnhwWWk5dGVYTnhiQXB6YjJOclpYUTlMM1poY2k5c2FXSXZiWGx6Y1d3dmJYbHpjV3d1YzI5amF3cDFjMlZ5UFcxNWMzRnNDaU1nUkdsellXSnNhVzVuSUhONWJXSnZiR2xqTFd4cGJtdHpJR2x6SUhKbFkyOXRiV1Z1WkdWa0lIUnZJSEJ5WlhabGJuUWdZWE56YjNKMFpXUWdjMlZqZFhKcGRIa2djbWx6YTNNS2MzbHRZbTlzYVdNdGJHbHVhM005TUFvS2MyVnlkbVZ5WDJsa1BURXdNQXBuZEdsa1gyMXZaR1U5VDA0S2JHOW5MV0pwYmoxdGVYTnhiQzFpYVc0S2JHOW5MWE5zWVhabExYVndaR0YwWlhNS1pXNW1iM0pqWlMxbmRHbGtMV052Ym5OcGMzUmxibU41Q2dwYmJYbHpjV3hrWDNOaFptVmRDbXh2WnkxbGNuSnZjajB2ZG1GeUwyeHZaeTl0ZVhOeGJHUXViRzluQ25CcFpDMW1hV3hsUFM5MllYSXZjblZ1TDIxNWMzRnNaQzl0ZVhOeGJHUXVjR2xrQ2c9PSINCiAgICAgICAgICB9DQogICAgICAgIF0sDQogICAgICAgICJiYWNrdXBfc3RhcnRlZCI6IDE1MjIyMTAyNzYsDQogICAgICAgICJwb3NpdGlvbiI6IDQ2ODU1LA0KICAgICAgICAidHlwZSI6ICJpbmNyZW1lbnRhbCIsDQogICAgICAgICJ3c3JlcF9wcm92aWRlcl92ZXJzaW9uIjogbnVsbA0KICAgICAgfQ0KICAgIH0sDQogICAgImRhaWx5Ijoge30sDQogICAgIndlZWtseSI6IHt9LA0KICAgICJtb250aGx5Ijoge30sDQogICAgInllYXJseSI6IHt9DQp9",
  "version": 1,
  "md5": "28a0da468054f5caa83a90fb3cac2eda"
}
    """
