from twindb_backup.source.mysqldump_source import MysqldumpSource


def test_costructor_calls_super():
    s = MysqldumpSource('foo', 'bar')
    assert s.run_type == 'foo'
    assert s.defaults_file == 'bar'
