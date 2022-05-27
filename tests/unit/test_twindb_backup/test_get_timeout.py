import pytest

from twindb_backup import get_timeout


@pytest.mark.parametrize(
    "run_type, timeout",
    [
        ("hourly", 3600 / 2),
        ("daily", 24 * 3600 / 2),
        ("weekly", 7 * 24 * 3600 / 2),
        ("monthly", 30 * 24 * 3600 / 2),
        ("yearly", 365 * 24 * 3600 / 2),
    ],
)
def test_get_timeout(run_type, timeout):
    assert get_timeout(run_type) == timeout
