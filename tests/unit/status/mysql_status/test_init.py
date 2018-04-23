import json
from base64 import b64encode

import pytest

from twindb_backup import INTERVALS
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.exceptions import CorruptedStatus
from twindb_backup.status.mysql_status import MySQLStatus


def test_init_creates_empty():
    status = MySQLStatus()
    assert status.version == 0
    for i in INTERVALS:
        assert getattr(status, i) == {}

    assert status.valid


def test_init_creates_instance(status_raw_content):
    status = MySQLStatus(status_raw_content)
    assert status.version == 0
    assert status.valid
    key = 'master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz'
    copy = MySQLCopy(
        'master1',
        'hourly',
        'mysql-2018-03-28_04_11_16.xbstream.gz',
        backup_started=1522210276,
        backup_finished=1522210295,
        binlog='mysql-bin.000001',
        parent='master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz',
        lsn=19903207,
        config=[
            {
                '/etc/my.cnf': """[mysqld]
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
        position=46855,
        type='incremental'

    )
    assert key in status.hourly
    assert status.hourly[key] == copy

def test_init_weekly_only():
    status = MySQLStatus(
        content=b64encode(
            json.dumps(
                {
                    u'daily': {},
                    u'hourly': {},
                    u'monthly': {},
                    u'weekly': {
                        u'foo/weekly/some_file.txt': {u'type': u'full'}},
                    u'yearly': {}
                }
            )
        )
    )
    assert not status.daily
    assert not status.hourly
    assert not status.monthly
    assert status.weekly
    assert not status.yearly


def test_init_invalid_json(invalid_status_raw_content):
    with pytest.raises(CorruptedStatus):
        MySQLStatus(invalid_status_raw_content)
