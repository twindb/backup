import mock

from twindb_backup import INTERVALS
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


def test_suffix():
    fs = MySQLSource(
        MySQLConnectInfo(
            '/foo/bar'
        ),
        INTERVALS[0],
        'full'
    )
    assert fs.suffix == 'xbstream'
    fs.suffix += '.gz'
    assert fs.suffix == 'xbstream.gz'
