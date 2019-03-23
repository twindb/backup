from twindb_backup import INTERVALS
from twindb_backup.configuration.run_intervals import RunIntervals


def test_run_intervals_default():
    ri = RunIntervals()
    for i in INTERVALS:
        assert getattr(ri, i) is True


def test_run_intervals():
    ri = RunIntervals(hourly=False)
    for i in INTERVALS:
        if i == 'hourly':
            assert getattr(ri, i) is False
        else:
            assert getattr(ri, i) is True
