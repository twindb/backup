import json
from base64 import b64encode

import pytest

from twindb_backup import INTERVALS, LOG, STATUS_FORMAT_VERSION
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.exceptions import CorruptedStatus
from twindb_backup.status.mysql_status import MySQLStatus


def test_init_creates_empty():
    status = MySQLStatus()
    assert status.version == STATUS_FORMAT_VERSION
    for i in INTERVALS:
        assert getattr(status, i) == {}


def test_init_creates_instance_from_old(deprecated_status_raw_content):
    status = MySQLStatus(deprecated_status_raw_content)
    assert status.version == STATUS_FORMAT_VERSION
    key = "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz"
    copy = MySQLCopy(
        "master1",
        "hourly",
        "mysql-2018-03-28_04_11_16.xbstream.gz",
        backup_started=1522210276,
        backup_finished=1522210295,
        binlog="mysql-bin.000001",
        parent="master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",
        lsn=19903207,
        config={
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
        },
        position=46855,
        type="incremental",
    )
    assert key in status.hourly
    LOG.debug("Copy %s: %r", copy.key, copy)
    LOG.debug("Copy from status %s: %r", key, status[key])
    assert status[key] == copy


def test_init_creates_instance_from_new(status_raw_content):
    status = MySQLStatus(status_raw_content)
    assert status.version == STATUS_FORMAT_VERSION
    key = "master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz"
    copy = MySQLCopy(
        "master1",
        "hourly",
        "mysql-2018-03-28_04_11_16.xbstream.gz",
        backup_started=1522210276,
        backup_finished=1522210295,
        binlog="mysql-bin.000001",
        parent="master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz",
        lsn=19903207,
        config={
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
        },
        position=46855,
        type="incremental",
    )
    assert key in status.hourly
    LOG.debug("Copy %s: %r", copy.key, copy)
    LOG.debug("Copy from status %s: %r", key, status[key])
    assert status[key] == copy


def test_init_raises_on_wrong_key():
    with pytest.raises(CorruptedStatus):
        MySQLStatus(
            content=b64encode(
                json.dumps(
                    {
                        "daily": {},
                        "hourly": {},
                        "monthly": {},
                        "weekly": {
                            "foo/weekly/some_file.txt": {"type": "full"}
                        },
                        "yearly": {},
                    }
                ).encode("utf-8")
            )
        )


def test_init_weekly_only():
    status = MySQLStatus(
        content=b64encode(
            json.dumps(
                {
                    "daily": {},
                    "hourly": {},
                    "monthly": {},
                    "weekly": {
                        "foo/weekly/mysql/some_file.txt": {"type": "full"}
                    },
                    "yearly": {},
                }
            ).encode("utf-8")
        )
    )
    assert not status.daily
    assert not status.hourly
    assert not status.monthly
    assert status.weekly
    assert not status.yearly


def test_init_invalid_json(invalid_deprecated_status_raw_content):
    with pytest.raises(CorruptedStatus):
        MySQLStatus(invalid_deprecated_status_raw_content)


def test_init_with_new_format(status_raw_content):
    status = MySQLStatus(status_raw_content)
    assert status.version == 1


def test_init_with_new_format_with_wrong_checksum(
    status_raw_content_with_invalid_hash,
):
    with pytest.raises(CorruptedStatus):
        MySQLStatus(status_raw_content_with_invalid_hash)


def test_init_example_0():
    content = b64encode(
        json.dumps(
            {
                "hourly": {"foo/hourly/mysql/some_file.txt": {"type": "full"}},
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            }
        ).encode("utf-8")
    )
    status = MySQLStatus(content=content)
    assert len(status.hourly) == 1
    assert len(status) == 1
    assert type(status["foo/hourly/mysql/some_file.txt"]) == MySQLCopy


def test_init_example_1():
    content = b64encode(
        json.dumps(
            {
                "hourly": {},
                "daily": {},
                "weekly": {"foo/weekly/mysql/some_file.txt": {"type": "full"}},
                "monthly": {},
                "yearly": {},
            }
        ).encode("utf-8")
    )
    status = MySQLStatus(content=content)
    assert len(status.weekly) == 1
    assert len(status) == 1
    assert type(status["foo/weekly/mysql/some_file.txt"]) == MySQLCopy
