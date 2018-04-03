# import json
import json
from copy import deepcopy
from pprint import pprint

from twindb_backup.status.status import Status


def test_serialize_doesnt_change_orignal(status_raw_content):
    status_original = Status(content=status_raw_content)
    status_original_before = deepcopy(status_original)
    status_original.serialize()
    assert status_original == status_original_before


def test_serialize(status_raw_content):
    status_original = Status(content=status_raw_content)
    print('\nOriginal status:')
    pprint(json.loads(str(status_original)))

    status_serialized = status_original.serialize()
    print('Serialized status:')
    pprint(status_serialized)

    status_converted = Status(content=status_serialized)
    print('Deserialized status:')
    pprint(json.loads(str(status_converted)))

    # status_original._yearly = {
    #     'foo': 'bar'
    # }
    assert status_original == status_converted
