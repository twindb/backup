import mock

from twindb_backup.destination.base_destination import BaseDestination


@mock.patch.object(BaseDestination, '_status_exists')
def test_get_status_empty(mock_status_exists):
    mock_status_exists.return_value = False

    dst = BaseDestination()
    assert dst.status() == {
        'hourly': {},
        'daily': {},
        'weekly': {},
        'monthly': {},
        'yearly': {}
    }
