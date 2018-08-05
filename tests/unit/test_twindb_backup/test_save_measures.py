import json

import os
import pytest

from twindb_backup import save_measures


@pytest.mark.parametrize('data', [
    (
        """
{"measures": [{"duration": 100, "start": 0, "finish": 100}]}
        """
    ),
    (
        """
{"measures": [{"duration": 150, "start": 0, "finish": 150}]}
        """
    ),
    (
        """
{"measures": [{"duration": 444, "start": 444, "finish": 888}]}
        """

    )
])
def test_save_measures_if_log_is_exist(data, tmpdir):
    log_file = tmpdir.join('log')
    log_file.write(data)
    save_measures(50, 100, str(log_file))
    with open(str(log_file)) as data_file:
        data = json.load(data_file)
    last_data = data['measures'][-1]
    assert last_data['duration'] == (last_data['finish'] - last_data['start'])


def test_save_measures_if_log_is_does_not_exist(tmpdir):
    log_file = tmpdir.join('log')
    save_measures(50, 100, str(log_file))
    assert os.path.isfile(str(log_file))
    with open(str(log_file)) as data_file:
        data = json.load(data_file)
    assert len(data['measures']) == 1
    last_data = data['measures'][-1]
    assert last_data['duration'] == (last_data['finish'] - last_data['start'])
