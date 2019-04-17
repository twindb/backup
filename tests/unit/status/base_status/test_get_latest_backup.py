from twindb_backup.status.base_status import BaseStatus


def test_latest_status_none():
    status = BaseStatus()
    assert status.latest_backup is None
