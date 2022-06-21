import json
from base64 import b64encode

import pytest

from twindb_backup.source.mysql_source import MySQLFlavor
from twindb_backup.status.mysql_status import MySQLStatus


@pytest.mark.parametrize(
    "status, expected_server_vendor",
    [
        (
            {
                "hourly": {
                    "foo/hourly/mysql/some_file.txt": {
                        "type": "full",
                        "server_vendor": "mariadb",
                    }
                },
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            MySQLFlavor.MARIADB,
        ),
        (
            {
                "hourly": {
                    "foo/hourly/mysql/some_file.txt": {
                        "type": "full",
                        "server_vendor": "percona",
                    }
                },
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            MySQLFlavor.PERCONA,
        ),
        (
            {
                "hourly": {
                    "foo/hourly/mysql/some_file.txt": {
                        "type": "full",
                        "server_vendor": "oracle",
                    }
                },
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            MySQLFlavor.ORACLE,
        ),
        (
            {
                "hourly": {"foo/hourly/mysql/some_file.txt": {"type": "full"}},
                "daily": {},
                "weekly": {},
                "monthly": {},
                "yearly": {},
            },
            MySQLFlavor.ORACLE,
        ),
    ],
)
def test_server_vendor(status, expected_server_vendor):

    istatus = MySQLStatus(content=b64encode(json.dumps(status).encode("utf-8")).decode("utf-8"))
    assert istatus.hourly["foo/hourly/mysql/some_file.txt"].server_vendor == expected_server_vendor
