import pytest
from twindb_backup.configuration.retention import RetentionPolicy


def test_retention_default():
    ret = RetentionPolicy()
    assert ret.hourly == 24
    assert ret.daily == 7
    assert ret.weekly == 4
    assert ret.monthly == 12
    assert ret.yearly == 3


def test_retention():
    assert RetentionPolicy(hourly=7).hourly == 7
    assert RetentionPolicy(daily=8).daily == 8

