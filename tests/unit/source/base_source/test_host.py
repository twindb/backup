"""Tests for BaseSource.host / get_prefix behaviour with server_name override."""

import socket

from twindb_backup.source.base_source import BaseSource


def test_host_defaults_to_hostname():
    src = BaseSource("daily")
    assert src.host == socket.gethostname()
    assert src.get_prefix() == f"{socket.gethostname()}/daily"


def test_host_uses_server_name_override():
    src = BaseSource("hourly", server_name="prod-primary-db")
    assert src.host == "prod-primary-db"
    assert src.get_prefix() == "prod-primary-db/hourly"


def test_empty_server_name_falls_back_to_hostname():
    src = BaseSource("daily", server_name="")
    assert src.host == socket.gethostname()


def test_none_server_name_falls_back_to_hostname():
    src = BaseSource("daily", server_name=None)
    assert src.host == socket.gethostname()
