from copy import deepcopy

from twindb_backup.status.binlog_status import BinlogStatus


def test_serialize_doesnt_change_orignal(raw_binlog_status):

    status_original = BinlogStatus(content=raw_binlog_status)
    status_original_before = deepcopy(status_original)

    assert status_original == status_original_before
    status_original.serialize()
    assert status_original == status_original_before


def test_serialize(raw_binlog_status):
    status_original = BinlogStatus(raw_binlog_status)
    status_converted = status_original.serialize()

    assert BinlogStatus(content=status_converted) == status_original
