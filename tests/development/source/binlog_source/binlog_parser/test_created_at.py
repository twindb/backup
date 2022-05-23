from twindb_backup.source.binlog_source import BinlogParser


def test_created_at():
    bp = BinlogParser("/tmp/mysql-bin.000001")
    assert bp.name == "mysql-bin.000001"
    assert bp.created_at == 1533403742
