import ConfigParser
import StringIO

import pytest

from twindb_backup.configuration import get_destination
from twindb_backup.destination.ssh import Ssh


def test__get_destination_ssh_valid_port(config_content):
    s_config = config_content.format(destination="ssh", port=4321)
    buf = StringIO.StringIO(s_config)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)
    dst = get_destination(config)
    assert isinstance(dst, Ssh)
    assert dst.client.port == 4321


def test__get_destination_ssh_valid_port_as_str(config_content):
    s_config = config_content.format(destination="ssh", port='1234')
    buf = StringIO.StringIO(s_config)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)
    dst = get_destination(config)
    assert isinstance(dst, Ssh)
    assert dst.client.port == 1234


def test__get_destination_ssh_invalid_port(config_content):
    s_config = config_content.format(destination="ssh", port='foo')
    buf = StringIO.StringIO(s_config)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)
    with pytest.raises(ValueError):
        get_destination(config)
