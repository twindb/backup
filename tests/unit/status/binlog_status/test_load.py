import pytest

from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.status.binlog_status import BinlogStatus


@pytest.mark.parametrize('status_json, copies', [
    (
        """{
            "master1/binlog/mysqlbin001.bin": {
                "time_created": "100500"
                },
            "master1/binlog/mysqlbin002.bin": {
                "time_created": "100501"
            }
        }
        """,
        [
            BinlogCopy('master1', 'mysqlbin001.bin', 100500),
            BinlogCopy('master1', 'mysqlbin002.bin', 100501),
        ]
    ),
    (
        """{
            "master1/binlog/mysqlbin001.bin": {
                "created_at": "100500"
                },
            "master1/binlog/mysqlbin002.bin": {
                "created_at": "100501"
            }
        }
        """,
        [
            BinlogCopy('master1', 'mysqlbin001.bin', 100500),
            BinlogCopy('master1', 'mysqlbin002.bin', 100501),
        ]
    ),
    (
        """{
            
            "master1/binlog/mysqlbin002.bin": {
                "created_at": "100501"
            },
            "master1/binlog/mysqlbin001.bin": {
                "created_at": "100500"
            }
        }
        """,
        [
            BinlogCopy('master1', 'mysqlbin001.bin', 100500),
            BinlogCopy('master1', 'mysqlbin002.bin', 100501),
        ]
    ),

])
def test_load(status_json, copies):
    status = BinlogStatus()
    assert status._load(status_json) == copies
