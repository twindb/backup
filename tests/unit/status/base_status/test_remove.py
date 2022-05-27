from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.status.base_status import BaseStatus


def test_remove_by_key():
    status = BaseStatus()
    copy = BaseCopy("foo", "bar")
    copy._source_type = "some_type"
    status.add(copy)
    assert len(status) == 1

    status.remove(copy.key)
    assert len(status) == 0


def test_remove_by_full_path():
    status = BaseStatus()
    copy = BaseCopy("foo", "bar")
    copy._source_type = "some_type"
    status.add(copy)
    assert len(status) == 1

    status.remove("blah" + copy.key)
    assert len(status) == 0
