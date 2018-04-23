import pytest

from twindb_backup.util import split_host_port


@pytest.mark.parametrize('pair, host, port', [
    (
        "10.20.31.1:3306",
        "10.20.31.1",
        3306
    ),
    (
        "10.20.31.1",
        "10.20.31.1",
        None
    ),
    (
        "10.20.31.1:",
        "10.20.31.1",
        None
    ),
    (
        None,
        None,
        None
    ),
    (
        "",
        None,
        None
    )
])
def test_split_host_port(pair, host, port):
    assert split_host_port(pair) == (host, port)
