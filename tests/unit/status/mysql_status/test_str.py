import json
from base64 import b64decode
from twindb_backup.status.mysql_status import MySQLStatus


def test_str_is_as_json(status_raw_content):
    status_original = MySQLStatus(content=status_raw_content)
    status_raw = b64decode(status_raw_content)
    status_raw_json = json.dumps(
        json.loads(status_raw),
        indent=4,
        sort_keys=True
    )
    status_original_json = str(status_original)
    assert status_raw_json == status_original_json
