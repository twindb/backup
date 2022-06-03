import pytest

from twindb_backup.configuration import CompressionConfig
from twindb_backup.configuration.exceptions import ConfigurationError


def test_init_default():
    cc = CompressionConfig()
    assert cc.program == "gzip"


def test_unsupported_program_raises():
    with pytest.raises(ConfigurationError):
        CompressionConfig(program="foo")
