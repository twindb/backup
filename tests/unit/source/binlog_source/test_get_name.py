import mock

from twindb_backup.source.binlog_source import BinlogSource


def test_get_name():

    src = BinlogSource(
        'daily',
        mock.Mock(),
        'foo-bin-log.0001'
    )

    assert src.get_name() == "foo-bin-log.0001"
