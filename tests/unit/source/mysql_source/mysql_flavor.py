from json import dumps

from twindb_backup.source.mariadb_source import MariaDBSource
from twindb_backup.source.mysql_source import MySQLFlavor, MySQLSource


def test_eq():
    assert MySQLFlavor.ORACLE == MySQLFlavor.ORACLE
    assert MySQLFlavor.ORACLE == "oracle"
    assert "oracle" == MySQLFlavor.ORACLE
    assert MySQLFlavor.ORACLE != MySQLFlavor.PERCONA


def test_json():
    assert dumps(MySQLFlavor.ORACLE) == '"oracle"'


def test_hash():
    mysql_src_map = {
        MySQLFlavor.MARIADB: MariaDBSource,
        MySQLFlavor.ORACLE: MySQLSource,
        MySQLFlavor.PERCONA: MySQLSource,
    }
    assert mysql_src_map[MySQLFlavor.MARIADB] is MariaDBSource
