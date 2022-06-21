import mock
import pytest
from mock.mock import call

from twindb_backup.clone import _get_mysql_service_name
from twindb_backup.destination.ssh import Ssh


@pytest.mark.parametrize(
    "side_effect, expected_name",
    [
        ([("0", ""), ("1\n", "")], "mysqld"),
        ([("0\n", ""), ("1\n", "")], "mysqld"),
        ([("\n0\n", ""), ("1\n", "")], "mysqld"),
        ([("1\n", ""), ("0\n", "")], "mysql"),
        ([("0", ""), ("0", ""), ("1", "")], "mariadb"),
    ],
)
def test_get_mysql_service_name(side_effect, expected_name):
    with mock.patch.object(Ssh, "execute_command", side_effect=side_effect) as mock_execute:
        assert _get_mysql_service_name(Ssh("foo")) == expected_name
        mock_execute.assert_has_calls(
            [call(f"systemctl list-units --full -all | grep -F '{expected_name}.service' | wc -l")]
        )
