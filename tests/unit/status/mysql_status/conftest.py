import json
from base64 import b64encode

import pytest


@pytest.fixture
def status_raw_content():
    """
    Returns a base64 encoded string with a JSON::

        {
              "monthly": {},
              "hourly": {
                "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz"
            : {
                  "backup_finished": 1522210295.521614,
                  "binlog": "mysql-bin.000001",
                  "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.x
            bstream.gz",
                  "lsn": 19903207,
                  "config": [
                    {
                      "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teX
            NxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCi
            MgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZX
            ZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2
            VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLX
            NsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3
            NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YX
            IvcnVuL215c3FsZC9teXNxbGQucGlkCg=="
                    }
                  ],
                  "backup_started": 1522210276.678018,
                  "position": 46855,
                  "type": "incremental"
                }
              },
              "daily": {
                "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz":
             {
                  "backup_finished": 1522210200.739319,
                  "binlog": "mysql-bin.000001",
                  "lsn": 19903199,
                  "type": "full",
                  "backup_started": 1522210193.291814,
                  "position": 46855,
                  "config": [
                    {
                      "/etc/my.cnf": "W215c3FsZF0KZGF0YWRpcj0vdmFyL2xpYi9teX
            NxbApzb2NrZXQ9L3Zhci9saWIvbXlzcWwvbXlzcWwuc29jawp1c2VyPW15c3FsCi
            MgRGlzYWJsaW5nIHN5bWJvbGljLWxpbmtzIGlzIHJlY29tbWVuZGVkIHRvIHByZX
            ZlbnQgYXNzb3J0ZWQgc2VjdXJpdHkgcmlza3MKc3ltYm9saWMtbGlua3M9MAoKc2
            VydmVyX2lkPTEwMApndGlkX21vZGU9T04KbG9nLWJpbj1teXNxbC1iaW4KbG9nLX
            NsYXZlLXVwZGF0ZXMKZW5mb3JjZS1ndGlkLWNvbnNpc3RlbmN5CgpbbXlzcWxkX3
            NhZmVdCmxvZy1lcnJvcj0vdmFyL2xvZy9teXNxbGQubG9nCnBpZC1maWxlPS92YX
            IvcnVuL215c3FsZC9teXNxbGQucGlkCg=="
                    }
                  ]
                }
              },
              "yearly": {},
              "weekly": {}
            }

    :return: base64 encoded string
    """
    return "eyJtb250aGx5Ijoge30sICJob3VybHkiOiB7Im1hc3RlcjEvaG91cmx5L215c3Fs" \
           "L215c3FsLTIwMTgtMDMtMjhfMDRfMTFfMTYueGJzdHJlYW0uZ3oiOiB7ImJhY2t1" \
           "cF9maW5pc2hlZCI6IDE1MjIyMTAyOTUuNTIxNjE0LCAiYmlubG9nIjogIm15c3Fs" \
           "LWJpbi4wMDAwMDEiLCAicGFyZW50IjogIm1hc3RlcjEvZGFpbHkvbXlzcWwvbXlz" \
           "cWwtMjAxOC0wMy0yOF8wNF8wOV81My54YnN0cmVhbS5neiIsICJsc24iOiAxOTkw" \
           "MzIwNywgImNvbmZpZyI6IFt7Ii9ldGMvbXkuY25mIjogIlcyMTVjM0ZzWkYwS1pH" \
           "RjBZV1JwY2owdmRtRnlMMnhwWWk5dGVYTnhiQXB6YjJOclpYUTlMM1poY2k5c2FX" \
           "SXZiWGx6Y1d3dmJYbHpjV3d1YzI5amF3cDFjMlZ5UFcxNWMzRnNDaU1nUkdsellX" \
           "SnNhVzVuSUhONWJXSnZiR2xqTFd4cGJtdHpJR2x6SUhKbFkyOXRiV1Z1WkdWa0lI" \
           "UnZJSEJ5WlhabGJuUWdZWE56YjNKMFpXUWdjMlZqZFhKcGRIa2djbWx6YTNNS2Mz" \
           "bHRZbTlzYVdNdGJHbHVhM005TUFvS2MyVnlkbVZ5WDJsa1BURXdNQXBuZEdsa1gy" \
           "MXZaR1U5VDA0S2JHOW5MV0pwYmoxdGVYTnhiQzFpYVc0S2JHOW5MWE5zWVhabExY" \
           "VndaR0YwWlhNS1pXNW1iM0pqWlMxbmRHbGtMV052Ym5OcGMzUmxibU41Q2dwYmJY" \
           "bHpjV3hrWDNOaFptVmRDbXh2WnkxbGNuSnZjajB2ZG1GeUwyeHZaeTl0ZVhOeGJH" \
           "UXViRzluQ25CcFpDMW1hV3hsUFM5MllYSXZjblZ1TDIxNWMzRnNaQzl0ZVhOeGJH" \
           "UXVjR2xrQ2c9PSJ9XSwgImJhY2t1cF9zdGFydGVkIjogMTUyMjIxMDI3Ni42Nzgw" \
           "MTgsICJwb3NpdGlvbiI6IDQ2ODU1LCAidHlwZSI6ICJpbmNyZW1lbnRhbCJ9fSwg" \
           "ImRhaWx5IjogeyJtYXN0ZXIxL2RhaWx5L215c3FsL215c3FsLTIwMTgtMDMtMjhf" \
           "MDRfMDlfNTMueGJzdHJlYW0uZ3oiOiB7ImJhY2t1cF9maW5pc2hlZCI6IDE1MjIy" \
           "MTAyMDAuNzM5MzE5LCAiYmlubG9nIjogIm15c3FsLWJpbi4wMDAwMDEiLCAibHNu" \
           "IjogMTk5MDMxOTksICJ0eXBlIjogImZ1bGwiLCAiYmFja3VwX3N0YXJ0ZWQiOiAx" \
           "NTIyMjEwMTkzLjI5MTgxNCwgInBvc2l0aW9uIjogNDY4NTUsICJjb25maWciOiBb" \
           "eyIvZXRjL215LmNuZiI6ICJXMjE1YzNGc1pGMEtaR0YwWVdScGNqMHZkbUZ5TDJ4" \
           "cFlpOXRlWE54YkFwemIyTnJaWFE5TDNaaGNpOXNhV0l2YlhsemNXd3ZiWGx6Y1d3" \
           "dWMyOWphd3AxYzJWeVBXMTVjM0ZzQ2lNZ1JHbHpZV0pzYVc1bklITjViV0p2Ykds" \
           "akxXeHBibXR6SUdseklISmxZMjl0YldWdVpHVmtJSFJ2SUhCeVpYWmxiblFnWVhO" \
           "emIzSjBaV1FnYzJWamRYSnBkSGtnY21semEzTUtjM2x0WW05c2FXTXRiR2x1YTNN" \
           "OU1Bb0tjMlZ5ZG1WeVgybGtQVEV3TUFwbmRHbGtYMjF2WkdVOVQwNEtiRzluTFdK" \
           "cGJqMXRlWE54YkMxaWFXNEtiRzluTFhOc1lYWmxMWFZ3WkdGMFpYTUtaVzVtYjNK" \
           "alpTMW5kR2xrTFdOdmJuTnBjM1JsYm1ONUNncGJiWGx6Y1d4a1gzTmhabVZkQ214" \
           "dlp5MWxjbkp2Y2owdmRtRnlMMnh2Wnk5dGVYTnhiR1F1Ykc5bkNuQnBaQzFtYVd4" \
           "bFBTOTJZWEl2Y25WdUwyMTVjM0ZzWkM5dGVYTnhiR1F1Y0dsa0NnPT0ifV19fSwg" \
           "InllYXJseSI6IHt9LCAid2Vla2x5Ijoge319"


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
