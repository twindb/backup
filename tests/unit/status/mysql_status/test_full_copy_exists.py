import json
from base64 import b64encode

import pytest

from twindb_backup.status.mysql_status import MySQLStatus


@pytest.mark.parametrize(
    "run_type, full_backup, status, expected",
    [
        (
            "daily",
            "weekly",
            {
                "hourly": {},
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            False,
        ),
        (
            "daily",
            "weekly",
            {
                "hourly": {"foo/hourly/mysql/some_file.txt": {"type": "full"}},
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            False,
        ),
        (
            "daily",  # run_type
            "weekly",  # full_backup frequency
            {
                "hourly": {},
                "daily": {},
                "weekly": {"foo/weekly/mysql/some_file.txt": {"type": "full"}},
                "monthly": {},
                "yearly": {},
            },
            True,  # Because weekly backup type is full
        ),
        (
            "daily",  # run_type
            "weekly",  # full_backup frequency
            {
                "hourly": {},
                "daily": {},
                "weekly": {
                    "foo/weekly/mysql/some_file.txt": {"type": "incremental"}
                },
                "monthly": {},
                "yearly": {},
            },
            False,  # Because weekly backup type is incremental
        ),
        (
            "hourly",  # run_type
            "daily",  # full_backup frequency
            {
                "hourly": {},
                "daily": {"foo/daily/mysql/some_file.txt": {"type": "full"}},
                "weekly": {
                    "foo/weekly/mysql/some_file.txt": {"type": "incremental"}
                },
                "monthly": {},
                "yearly": {},
            },
            True,  # Because daily backup type is full
        ),
        (
            "hourly",  # run_type
            "daily",  # full_backup frequency
            {
                "hourly": {},
                "daily": {
                    "foo/daily/mysql/some_file.txt": {"type": "incremental"}
                },
                "weekly": {
                    "foo/weekly/mysql/some_file.txt": {"type": "incremental"}
                },
                "monthly": {},
                "yearly": {},
            },
            False  # Because daily backup type is incremental and
            # weekly is incremental
        ),
        (
            "hourly",  # run_type
            "daily",  # full_backup frequency
            {
                "hourly": {},
                "daily": {
                    "foo/daily/mysql/some_file.txt": {"type": "incremental"}
                },
                "weekly": {"foo/weekly/mysql/some_file.txt": {"type": "full"}},
                "monthly": {},
                "yearly": {},
            },
            True  # Because weekly backup type is incremental but
            # weekly is full
        ),
        (
            "hourly",
            "daily",
            {
                "hourly": {},
                "daily": {},
                "weekly": {"foo/weekly/mysql/some_file.txt": {"type": "full"}},
                "monthly": {},
                "yearly": {},
            },
            True,
        ),
        (
            "hourly",
            "daily",
            {
                "hourly": {},
                "daily": {},
                "weekly": {
                    "foo/weekly/mysql/some_file.txt": {"type": "incremental"}
                },
                "monthly": {},
                "yearly": {},
            },
            False,
        ),
        (
            "hourly",
            "weekly",
            {
                "hourly": {},
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            False,
        ),
        (
            "hourly",
            "weekly",
            {
                "daily": {
                    "master.box/daily/mysql/mysql-2016-11-20_04_13_10.xbstream.gz": {
                        "binlog": "mysql-bin.000001",
                        "lsn": 19630043,
                        "position": 44726,
                        "type": "full",
                    }
                },
                "hourly": {
                    "master.box/hourly/mysql/mysql-2016-11-20_04_12_26.xbstream.gz": {
                        "binlog": "mysql-bin.000001",
                        "lsn": 19630035,
                        "position": 44726,
                        "type": "full",
                    },
                    "master.box/hourly/mysql/mysql-2016-11-20_04_13_36.xbstream.gz": {
                        "binlog": "mysql-bin.000001",
                        "lsn": 19630051,
                        "parent": "master.box/daily/mysql/mysql-2016-11-20_04_13_10.xbstream.gz",
                        "position": 44726,
                        "type": "incremental",
                    },
                },
                "monthly": {},
                "weekly": {},
                "yearly": {},
            },
            True,
        ),
    ],
)
def test_full_copy_exists(run_type, full_backup, status, expected):

    istatus = MySQLStatus(
        content=b64encode(json.dumps(status).encode("utf-8")).decode("utf-8")
    )
    assert istatus.full_copy_exists(run_type) == expected
