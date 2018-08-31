from twindb_backup.status.periodic_status import PeriodicStatus


def test_eq_empty():
    status1 = PeriodicStatus()
    status2 = PeriodicStatus()
    assert status1 == status2
