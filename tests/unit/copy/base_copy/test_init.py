import pytest

from twindb_backup.copy.base_copy import BaseCopy


@pytest.mark.parametrize("host, fname", [
    (
        "test_host",
        "test_file"
    ),
])
def test_init(host, fname):
    instance = BaseCopy(host, fname)
    assert instance._host == host
    assert instance._name == fname
    assert instance._source_type is None

