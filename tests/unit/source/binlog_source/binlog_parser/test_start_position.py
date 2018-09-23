from twindb_backup.source.binlog_source import BinlogParser


def test_start_position(mysql_bin_000001):
    bp = BinlogParser(mysql_bin_000001)
    assert bp.start_position == 4
