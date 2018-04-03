import json
from pprint import pprint

from twindb_backup.status.status import Status


def test_deserialize(status_raw_content):
    status = Status().deserialize(status_raw_content)
    pprint(json.loads(str(status)))
    assert status == {
        "monthly": {},
        "hourly": {
            "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz": {
                "backup_finished": 1522210295.521614,
                "binlog": "mysql-bin.000001",
                "parent": "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",
                "lsn": 19903207,
                "config": [
                    {
                        "/etc/my.cnf": """[mysqld]
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
user=mysql
# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0

server_id=100
gtid_mode=ON
log-bin=mysql-bin
log-slave-updates
enforce-gtid-consistency

[mysqld_safe]
log-error=/var/log/mysqld.log
pid-file=/var/run/mysqld/mysqld.pid
"""
                    }
                ],
                "backup_started": 1522210276.678018,
                "position": 46855,
                "type": "incremental"
            }
        },
        "daily": {
            "master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz": {
                "backup_finished": 1522210200.739319,
                "binlog": "mysql-bin.000001",
                "lsn": 19903199,
                "type": "full",
                "backup_started": 1522210193.291814,
                "position": 46855,
                "config": [
                    {
                        "/etc/my.cnf": """[mysqld]
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
user=mysql
# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0

server_id=100
gtid_mode=ON
log-bin=mysql-bin
log-slave-updates
enforce-gtid-consistency

[mysqld_safe]
log-error=/var/log/mysqld.log
pid-file=/var/run/mysqld/mysqld.pid
"""
                    }
                ]
            }
        },
        "yearly": {},
        "weekly": {}
    }
