from twindb_backup.status.base_status import BaseStatus


def test_latest_status_none():
    status = BaseStatus()
    assert status.get_latest_backup() is None
