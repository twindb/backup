import mock
import pytest

from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.copy.exceptions import UnknownSourceType


def test_key_raised_error_in_abstract_class():
    instance = BaseCopy("host", "fname")
    with pytest.raises(UnknownSourceType):
        # noinspection PyStatementEffect
        instance.key


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "extra_path, key",
    [(None, "foo/foo-type/bar"), ("foo-path", "foo/foo-path/foo-type/bar")],
)
@mock.patch.object(BaseCopy, "_extra_path")
def test_key(mock_extra_path, extra_path, key):

    copy = BaseCopy("foo", "bar")
    copy._source_type = "foo-type"

    # noinspection PyPropertyAccess
    copy._extra_path = extra_path

    assert copy.key == key
