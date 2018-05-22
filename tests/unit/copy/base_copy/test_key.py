import pytest

from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.copy.exceptions import UnknownSourceType


def test_key_raised_error_in_abstract_class():
    instance = BaseCopy("host", "fname")
    with pytest.raises(UnknownSourceType):
        key = instance.key
