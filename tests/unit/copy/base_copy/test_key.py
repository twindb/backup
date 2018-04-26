import pytest

from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.copy.exceptions import UnknownSourceType


def test_key_raises():
    backup_copy = BaseCopy('foo', 'daily', 'some_file.txt')

    with pytest.raises(UnknownSourceType):
        assert backup_copy.key
