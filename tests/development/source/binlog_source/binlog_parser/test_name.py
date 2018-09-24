from twindb_backup.source.binlog_source import BinlogParser


def test_name():
    bp = BinlogParser('/var/lib/foo/bar')
    assert bp.name == 'bar'
