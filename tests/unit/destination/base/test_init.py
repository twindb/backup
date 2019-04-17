import pytest

from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import DestinationError


def test_init_raises():
    with pytest.raises(DestinationError):
        BaseDestination(None)
