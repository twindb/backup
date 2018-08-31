import pytest

from twindb_backup.copy.binlog_copy import BinlogCopy


@pytest.mark.parametrize("host, fname, time_created, copy_repr", [
    (
        "test_host",
        "test_file",
        100500,
        'BinlogCopy(test_host/binlog/test_file)'
    ),
    (
        "test_host2",
        "test_file2",
        1005002,
        'BinlogCopy(test_host2/binlog/test_file2)'
    ),
])
def test_repr(host, fname, time_created, copy_repr):
    instance = BinlogCopy(host, fname, time_created)
    assert repr(instance) == copy_repr


@pytest.mark.parametrize("host, fname, time_created, copy_as_str", [
    (
        "test_host",
        "test_file",
        100500,
        'BinlogCopy: file name: test_host/binlog/test_file, created at: 100500'
    ),
    (
        "test_host2",
        "test_file2",
        1005002,
        'BinlogCopy: '
        'file name: test_host2/binlog/test_file2, created at: 1005002'
    ),
])
def test_str(host, fname, time_created, copy_as_str):
    instance = BinlogCopy(host, fname, time_created)
    assert str(instance) == copy_as_str
