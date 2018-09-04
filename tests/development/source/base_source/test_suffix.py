from twindb_backup.source.base_source import BaseSource


def test_suffix_update():
    src = BaseSource('daily')
    src.suffix = 'xbstream'
    assert src.suffix == 'xbstream'
    src.suffix += '.gz'
    assert src.suffix == 'xbstream.gz'
