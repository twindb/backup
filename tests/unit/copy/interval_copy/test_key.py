import pytest

from twindb_backup.copy.interval_copy import IntervalCopy
from twindb_backup.copy.exceptions import UnknownSourceType


def test_key_raises():
    backup_copy = IntervalCopy('foo', 'daily', 'some_file.txt')

    with pytest.raises(UnknownSourceType):
        assert backup_copy.key
