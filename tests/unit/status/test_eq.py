from twindb_backup.status.status import Status


def test_eq_empty():
    status = Status()
    assert status == {
        "hourly": {},
        "daily": {},
        "weekly": {},
        "monthly": {},
        "yearly": {}
    }
    assert status.valid
