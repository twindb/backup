import pytest

from tests.unit.status.mysql_status.conftest import deprecated_status_raw_content
from twindb_backup.status.mysql_status import MySQLStatus


@pytest.fixture
def filled_mysql_status():
    return MySQLStatus(deprecated_status_raw_content())
