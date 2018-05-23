from twindb_backup.status.periodic_status import PeriodicStatus


def test_init():
    instance = PeriodicStatus()
    assert instance.hourly == {}
    assert instance.daily == {}
    assert instance.weekly == {}
    assert instance.yearly == {}
    assert instance.monthly == {}


