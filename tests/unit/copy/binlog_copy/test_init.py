import pytest

from twindb_backup.copy.binlog_copy import BinlogCopy


@pytest.mark.parametrize("host, fname, time_created", [
    (
        "test_host",
        "test_file",
        100500
    ),
    (
        "test_host2",
        "test_file2",
        1005002
    ),
])
def test_init(host, fname, time_created):
    instance = BinlogCopy(host, fname, time_created)
    assert instance._time_created == time_created
    assert instance._name == fname
    assert instance._source_type == 'binlog'
    assert instance._extra_path == None
