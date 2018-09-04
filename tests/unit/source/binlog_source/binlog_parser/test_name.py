from twindb_backup.source.binlog_source import BinlogParser


def test_name(mysql_bin_000001):
    bp = BinlogParser(mysql_bin_000001)
    assert bp.name == 'mysql-bin.000001'
