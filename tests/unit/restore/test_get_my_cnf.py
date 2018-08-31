from pprint import pprint

from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.restore import get_my_cnf
from twindb_backup.status.mysql_status import MySQLStatus


def test_get_my_cnf():
    status = MySQLStatus(content="""{
    "md5": "1939bce689ef7d070beae0860c885caf", 
    "status": "eyJtb250aGx5Ijoge30sICJob3VybHkiOiB7fSwgInllYXJseSI6IHt9LCAiZGFpbHkiOiB7Im1hc3RlcjFfMS9kYWlseS9teXNxbC9teXNxbC0yMDE4LTA4LTA1XzAyXzMyXzE0Lnhic3RyZWFtLmd6LmdwZyI6IHsiZ2FsZXJhIjogZmFsc2UsICJiaW5sb2ciOiAibXlzcWwtYmluLjAwMDAwMiIsICJydW5fdHlwZSI6ICJkYWlseSIsICJuYW1lIjogIm15c3FsLTIwMTgtMDgtMDVfMDJfMzJfMTQueGJzdHJlYW0uZ3ouZ3BnIiwgInBhcmVudCI6IG51bGwsICJsc24iOiAyNTUxMTg1LCAidHlwZSI6ICJmdWxsIiwgImJhY2t1cF9maW5pc2hlZCI6IDE1MzM0MzYzMzgsICJ3c3JlcF9wcm92aWRlcl92ZXJzaW9uIjogbnVsbCwgImhvc3QiOiAibWFzdGVyMV8xIiwgImJhY2t1cF9zdGFydGVkIjogMTUzMzQzNjMzMywgInBvc2l0aW9uIjogMTA1NCwgImNvbmZpZyI6IFt7Ii9ldGMvbXkuY25mIjogIlcyMTVjM0ZzWkYwS1pHRjBZV1JwY2owdmRtRnlMMnhwWWk5dGVYTnhiQXB6YjJOclpYUTlMM1poY2k5c2FXSXZiWGx6Y1d3dmJYbHpjV3d1YzI5amF3cDFjMlZ5UFcxNWMzRnNDaU1nUkdsellXSnNhVzVuSUhONWJXSnZiR2xqTFd4cGJtdHpJR2x6SUhKbFkyOXRiV1Z1WkdWa0lIUnZJSEJ5WlhabGJuUWdZWE56YjNKMFpXUWdjMlZqZFhKcGRIa2djbWx6YTNNS2MzbHRZbTlzYVdNdGJHbHVhM005TUFvS2MyVnlkbVZ5WDJsa1BURXdNQXBzYjJjdFltbHVQVzE1YzNGc0xXSnBiZ3BzYjJjdGMyeGhkbVV0ZFhCa1lYUmxjd29LVzIxNWMzRnNaRjl6WVdabFhRcHNiMmN0WlhKeWIzSTlMM1poY2k5c2IyY3ZiWGx6Y1d4a0xteHZad3B3YVdRdFptbHNaVDB2ZG1GeUwzSjFiaTl0ZVhOeGJHUXZiWGx6Y1d4a0xuQnBaQW89In1dfX0sICJ3ZWVrbHkiOiB7fX0=", 
    "version": 1}
    """)
    print(status)
    key = 'master1_1/daily/mysql/mysql-2018-08-05_02_32_14.xbstream.gz.gpg'
    for path, content in get_my_cnf(status, key):
        assert path == "/etc/my.cnf"
        assert content == """[mysqld]
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
user=mysql
# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0

server_id=100
log-bin=mysql-bin
log-slave-updates

[mysqld_safe]
log-error=/var/log/mysqld.log
pid-file=/var/run/mysqld/mysqld.pid
"""


def test_get_my_cnf_2_cnf(tmpdir):
    status = MySQLStatus()
    mycnf_1 = tmpdir.join('my-1.cnf')
    mycnf_1.write('some_content_1')
    mycnf_2 = tmpdir.join('my-2.cnf')
    mycnf_2.write('some_content_2')

    backup_copy = MySQLCopy(
        'master1', 'daily', 'foo.txt',
        binlog='binlog1',
        position=101,
        type='full',
        lsn=1230,
        backup_started=123,
        backup_finished=456,
        config_files=[str(mycnf_1), str(mycnf_2)]
    )
    status.add(backup_copy)
    expected = {
        str(mycnf_1): 'some_content_1',
        str(mycnf_2): 'some_content_2'
    }
    for path, content in get_my_cnf(status, backup_copy.key):
        assert path in expected
        expected_value = expected.pop(path)
        assert content == expected_value

    assert expected == {}
