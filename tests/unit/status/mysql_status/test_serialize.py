from copy import deepcopy

from twindb_backup.status.mysql_status import MySQLStatus


def test_serialize_doesnt_change_orignal(status_raw_content):
    status_original = MySQLStatus(content=status_raw_content)
    status_original_before = deepcopy(status_original)
    status_original.serialize()
    assert status_original == status_original_before


def test_serialize(status_raw_content):
    status_original = MySQLStatus(content=status_raw_content)
    print('\nOriginal status:\n%s' % status_original)

    status_serialized = status_original.serialize()
    print('Serialized status:\n%s' % status_serialized)

    status_converted = MySQLStatus(content=status_serialized)
    print('Deserialized status:\n%s' % status_converted)

    assert status_original == status_converted
