import pytest

from twindb_backup.source.binlog_source import BinlogParser
from twindb_backup.source.exceptions import BinlogSourceError


def test_created_at(mysql_bin_000001):
    bp = BinlogParser(mysql_bin_000001)
    assert bp.created_at == 1533403742


def test_created_at_raises():
    bp = BinlogParser('foo')
    with pytest.raises(BinlogSourceError):
        assert bp.created_at
