import pytest

from twindb_backup.copy.exceptions import UnknownSourceType
from twindb_backup.copy.periodic_copy import PeriodicCopy


def test_key_raises():
    backup_copy = PeriodicCopy("foo", "daily", "some_file.txt")

    with pytest.raises(UnknownSourceType):
        assert backup_copy.key
