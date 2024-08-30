import pytest

from twindb_backup.configuration.destinations.az import AZConfig

from .util import AZConfigParams


def test_initialization_success():
    """Test initialization of AZConfig with all parameters set."""
    p = AZConfigParams()
    c = AZConfig(**dict(p))
    assert c.connection_string == p.connection_string
    assert c.container_name == p.container_name
    assert c.chunk_size == p.chunk_size
    assert c.remote_path == p.remote_path


def test_initialization_success_defaults():
    """Test initialization of AZConfig with only required parameters set and ensure default values."""
    p = AZConfigParams(only_required=True)
    c = AZConfig(**dict(p))
    assert c.connection_string == p.connection_string
    assert c.container_name == p.container_name
    assert c.chunk_size == 4 * 1024 * 1024
    assert c.remote_path == "/"


def test_invalid_params():
    """Test initialization of AZConfig with invalid parameters."""
    with pytest.raises(ValueError):
        AZConfig(
            connection_string="test_connection_string", container_name="test_container", chunk_size="invalid_chunk_size"
        )
    with pytest.raises(ValueError):
        AZConfig(connection_string="test_connection_string", container_name="test_container", remote_path=1)
    with pytest.raises(TypeError):
        AZConfig(connection_string="test_connection_string")
