import json
from copy import deepcopy

from twindb_backup.status.mysql_status import MySQLStatus


def test_serialize_doesnt_change_orignal(status_raw_content, deprecated_status_raw_content):

    status_original = MySQLStatus(content=deprecated_status_raw_content)
    status_original_before = deepcopy(status_original)
    assert status_original == status_original_before
    status_original.serialize()
    assert status_original == status_original_before

    status_original = MySQLStatus(content=status_raw_content)
    status_original_before = deepcopy(status_original)
    assert status_original == status_original_before
    status_original.serialize()
    assert status_original == status_original_before


def test_serialize_old(deprecated_status_raw_content):
    status_original = MySQLStatus(content=deprecated_status_raw_content)
    print("\nOriginal status:\n%s" % status_original)

    status_serialized = status_original.serialize()
    print("Serialized status:\n%s" % status_serialized)

    status_converted = MySQLStatus(content=status_serialized)
    print("Deserialized status:\n%s" % status_converted)

    assert status_original == status_converted


def test_serialize_new(status_raw_content):
    status_original = MySQLStatus(content=status_raw_content)
    print("\nOriginal status:\n%s" % status_original)

    status_serialized = status_original.serialize()
    print("Serialized status:\n%s" % status_serialized)

    status_converted = MySQLStatus(content=status_serialized)
    print("Deserialized status:\n%s" % status_converted)

    assert status_original == status_converted


def test_serialize_is_valid(status_raw_content):
    status = MySQLStatus(content=status_raw_content)
    serialized_status = status.serialize()
    dict_from_status = json.loads(serialized_status)
    assert "status" in dict_from_status
    assert "version" in dict_from_status
    assert "md5" in dict_from_status
