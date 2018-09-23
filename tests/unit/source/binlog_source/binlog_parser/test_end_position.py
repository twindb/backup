from twindb_backup.source.binlog_source import BinlogParser


def test_end_position(mysql_bin_000001):
    bp = BinlogParser(mysql_bin_000001)
    assert bp.end_position == 46860
