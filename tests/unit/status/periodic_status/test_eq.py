from twindb_backup.status.periodic_status import PeriodicStatus

def test_eq_empty():
    status1 = PeriodicStatus()
    status2 = PeriodicStatus()
    assert status1 == status2
    assert status1.valid
    assert status2.valid
