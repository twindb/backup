import pytest

from twindb_backup.copy.exceptions import WrongInputData
from twindb_backup.copy.mysql_copy import MySQLCopy


def test_init_raises_if_name_is_not_relative():
    with pytest.raises(WrongInputData):
        MySQLCopy('foo', 'daily', 'some/non/relative/path', type='full')
