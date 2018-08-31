import json
from base64 import b64encode

import pytest

from twindb_backup import setup_logging, LOG

setup_logging(LOG, debug=True)


@pytest.fixture
def deprecated_status_raw_content():
    """
    Returns a base64 encoded string with a JSON::

        {
              "monthly": {},
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
              "daily": {
                "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz": {
                  "backup_finished": 1522210200,
                  "binlog": "mysql-bin.000001",
                  "lsn": 19903199,
                  "parent": null,
                  "type": "full",
                  "wsrep_provider_version": null,
                  "backup_started": 1522210193,
                  "galera": false,
                  "position": 46855,
                  "config": [
                    {
                      "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teXNxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCiMgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZXZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YXIvcnVuL215c3FsZC9teXNxbGQucGlkCg=="
                    }
                  ]
                }
              },
              "yearly": {},
              "weekly": {}
            }

    :return: base64 encoded string
    """

    return """
    ICAgICAgICB7DQogICAgICAgICAgICAgICJtb250aGx5Ijoge30sDQogICAgICAgICAgICAgIC
    Job3VybHkiOiB7DQogICAgICAgICAgICAgICAgIm1hc3RlcjEvaG91cmx5L215c3FsL215c3Fs
    LTIwMTgtMDMtMjhfMDRfMTFfMTYueGJzdHJlYW0uZ3oiOiB7DQogICAgICAgICAgICAgICAgIC
    AiYmFja3VwX2ZpbmlzaGVkIjogMTUyMjIxMDI5NSwNCiAgICAgICAgICAgICAgICAgICJiaW5s
    b2ciOiAibXlzcWwtYmluLjAwMDAwMSIsDQogICAgICAgICAgICAgICAgICAicGFyZW50IjogIm
    1hc3RlcjEvZGFpbHkvbXlzcWwvbXlzcWwtMjAxOC0wMy0yOF8wNF8wOV81My54YnN0cmVhbS5n
    eiIsDQogICAgICAgICAgICAgICAgICAibHNuIjogMTk5MDMyMDcsDQogICAgICAgICAgICAgIC
    AgICAiZ2FsZXJhIjogZmFsc2UsDQogICAgICAgICAgICAgICAgICAiY29uZmlnIjogWw0KICAg
    ICAgICAgICAgICAgICAgICB7DQogICAgICAgICAgICAgICAgICAgICAgIi9ldGMvbXkuY25mIj
    ogIlcyMTVjM0ZzWkYwS1pHRjBZV1JwY2owdmRtRnlMMnhwWWk5dGVYTnhiQXB6YjJOclpYUTlM
    M1poY2k5c2FXSXZiWGx6Y1d3dmJYbHpjV3d1YzI5amF3cDFjMlZ5UFcxNWMzRnNDaU1nUkdsel
    lXSnNhVzVuSUhONWJXSnZiR2xqTFd4cGJtdHpJR2x6SUhKbFkyOXRiV1Z1WkdWa0lIUnZJSEJ5
    WlhabGJuUWdZWE56YjNKMFpXUWdjMlZqZFhKcGRIa2djbWx6YTNNS2MzbHRZbTlzYVdNdGJHbH
    VhM005TUFvS2MyVnlkbVZ5WDJsa1BURXdNQXBuZEdsa1gyMXZaR1U5VDA0S2JHOW5MV0pwYmox
    dGVYTnhiQzFpYVc0S2JHOW5MWE5zWVhabExYVndaR0YwWlhNS1pXNW1iM0pqWlMxbmRHbGtMV0
    52Ym5OcGMzUmxibU41Q2dwYmJYbHpjV3hrWDNOaFptVmRDbXh2WnkxbGNuSnZjajB2ZG1GeUwy
    eHZaeTl0ZVhOeGJHUXViRzluQ25CcFpDMW1hV3hsUFM5MllYSXZjblZ1TDIxNWMzRnNaQzl0ZV
    hOeGJHUXVjR2xrQ2c9PSINCiAgICAgICAgICAgICAgICAgICAgfQ0KICAgICAgICAgICAgICAg
    ICAgXSwNCiAgICAgICAgICAgICAgICAgICJiYWNrdXBfc3RhcnRlZCI6IDE1MjIyMTAyNzYsDQ
    ogICAgICAgICAgICAgICAgICAicG9zaXRpb24iOiA0Njg1NSwNCiAgICAgICAgICAgICAgICAg
    ICJ0eXBlIjogImluY3JlbWVudGFsIiwNCiAgICAgICAgICAgICAgICAgICJ3c3JlcF9wcm92aW
    Rlcl92ZXJzaW9uIjogbnVsbA0KICAgICAgICAgICAgICAgIH0NCiAgICAgICAgICAgICAgfSwN
    CiAgICAgICAgICAgICAgImRhaWx5Ijogew0KICAgICAgICAgICAgICAgICJtYXN0ZXIxL2RhaW
    x5L215c3FsL215c3FsLTIwMTgtMDMtMjhfMDRfMDlfNTMueGJzdHJlYW0uZ3oiOiB7DQogICAg
    ICAgICAgICAgICAgICAiYmFja3VwX2ZpbmlzaGVkIjogMTUyMjIxMDIwMCwNCiAgICAgICAgIC
    AgICAgICAgICJiaW5sb2ciOiAibXlzcWwtYmluLjAwMDAwMSIsDQogICAgICAgICAgICAgICAg
    ICAibHNuIjogMTk5MDMxOTksDQogICAgICAgICAgICAgICAgICAicGFyZW50IjogbnVsbCwNCi
    AgICAgICAgICAgICAgICAgICJ0eXBlIjogImZ1bGwiLA0KICAgICAgICAgICAgICAgICAgIndz
    cmVwX3Byb3ZpZGVyX3ZlcnNpb24iOiBudWxsLA0KICAgICAgICAgICAgICAgICAgImJhY2t1cF
    9zdGFydGVkIjogMTUyMjIxMDE5MywNCiAgICAgICAgICAgICAgICAgICJnYWxlcmEiOiBmYWxz
    ZSwNCiAgICAgICAgICAgICAgICAgICJwb3NpdGlvbiI6IDQ2ODU1LA0KICAgICAgICAgICAgIC
    AgICAgImNvbmZpZyI6IFsNCiAgICAgICAgICAgICAgICAgICAgew0KICAgICAgICAgICAgICAg
    ICAgICAgICIvZXRjL215LmNuZiI6ICJXMjE1YzNGc1pGMEtaR0YwWVdScGNqMHZkbUZ5TDJ4cF
    lpOXRlWE54YkFwemIyTnJaWFE5TDNaaGNpOXNhV0l2YlhsemNXd3ZiWGx6Y1d3dWMyOWphd3Ax
    YzJWeVBXMTVjM0ZzQ2lNZ1JHbHpZV0pzYVc1bklITjViV0p2YkdsakxXeHBibXR6SUdseklISm
    xZMjl0YldWdVpHVmtJSFJ2SUhCeVpYWmxiblFnWVhOemIzSjBaV1FnYzJWamRYSnBkSGtnY21s
    emEzTUtjM2x0WW05c2FXTXRiR2x1YTNNOU1Bb0tjMlZ5ZG1WeVgybGtQVEV3TUFwbmRHbGtYMj
    F2WkdVOVQwNEtiRzluTFdKcGJqMXRlWE54YkMxaWFXNEtiRzluTFhOc1lYWmxMWFZ3WkdGMFpY
    TUtaVzVtYjNKalpTMW5kR2xrTFdOdmJuTnBjM1JsYm1ONUNncGJiWGx6Y1d4a1gzTmhabVZkQ2
    14dlp5MWxjbkp2Y2owdmRtRnlMMnh2Wnk5dGVYTnhiR1F1Ykc5bkNuQnBaQzFtYVd4bFBTOTJZ
    WEl2Y25WdUwyMTVjM0ZzWkM5dGVYTnhiR1F1Y0dsa0NnPT0iDQogICAgICAgICAgICAgICAgIC
    AgIH0NCiAgICAgICAgICAgICAgICAgIF0NCiAgICAgICAgICAgICAgICB9DQogICAgICAgICAg
    ICAgIH0sDQogICAgICAgICAgICAgICJ5ZWFybHkiOiB7fSwNCiAgICAgICAgICAgICAgIndlZW
    tseSI6IHt9DQogICAgICAgICAgICB9
"""


@pytest.fixture
def invalid_deprecated_status_raw_content():
    """
    Returns a base64 encoded string with a JSON::

        {
              "monthly": {},
              "hourly": {
                "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {
                  "backup_finished": 1522210295,
                  "binlog": "mysql-bin.000001",
                  "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",
                  "lsn": 19903207,
                  "galera": false,
                  "config": [
                    {
                      "/etc/my.cnf": "Foo-bar-bah"
                    }
                  ],
            }

    :return: base64 encoded string
    """

    return """
    ICAgICAgICB7DQogICAgICAgICAgICAgICJtb250aGx5Ijoge30sDQogICAgIC
    AgICAgICAgICJob3VybHkiOiB7DQogICAgICAgICAgICAgICAgIm1hc3RlcjEv
    aG91cmx5L215c3FsL215c3FsLTIwMTgtMDMtMjhfMDRfMTFfMTYueGJzdHJlYW
    0uZ3oiOiB7DQogICAgICAgICAgICAgICAgICAiYmFja3VwX2ZpbmlzaGVkIjog
    MTUyMjIxMDI5NSwNCiAgICAgICAgICAgICAgICAgICJiaW5sb2ciOiAibXlzcW
    wtYmluLjAwMDAwMSIsDQogICAgICAgICAgICAgICAgICAicGFyZW50IjogIm1h
    c3RlcjEvZGFpbHkvbXlzcWwvbXlzcWwtMjAxOC0wMy0yOF8wNF8wOV81My54Yn
    N0cmVhbS5neiIsDQogICAgICAgICAgICAgICAgICAibHNuIjogMTk5MDMyMDcs
    DQogICAgICAgICAgICAgICAgICAiZ2FsZXJhIjogZmFsc2UsDQogICAgICAgIC
    AgICAgICAgICAiY29uZmlnIjogWw0KICAgICAgICAgICAgICAgICAgICB7DQog
    ICAgICAgICAgICAgICAgICAgICAgIi9ldGMvbXkuY25mIjogIkZvby1iYXItYm
    FoIg0KICAgICAgICAgICAgICAgICAgICB9DQogICAgICAgICAgICAgICAgICBd
    LA0KICAgICAgICAgICAgfQ==
"""


@pytest.fixture
def status_raw_empty():
    return b64encode(
        json.dumps(
            {
                "hourly": {},
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {}
            }
        )
    )


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


@pytest.fixture
def status_raw_content_with_invalid_hash():
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
  "md5": "28a0da468054f5caa83a90fb3cac2beda"
}
    """
