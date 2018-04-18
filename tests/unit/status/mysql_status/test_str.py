import json

import pytest

from twindb_backup.status.mysql_status import MySQLStatus


def test_str_is_as_json(status_raw_content):
    status_original = MySQLStatus(content=status_raw_content)
    try:
        original_json = str(status_original)
        json.loads(original_json)
    except ValueError:
        pytest.fail("Cant convert to JSON")
