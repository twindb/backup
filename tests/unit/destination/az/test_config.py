from dataclasses import asdict

import pytest

from twindb_backup.configuration.destinations.az import (
    AUTH_MODE_CONNECTION_STRING,
    AUTH_MODE_MANAGED_IDENTITY,
    AZClientConfig,
    AZConfig,
    drop_empty_dict_factory,
)

from .util import AZClientConfigParams, AZConfigParams


def test_initialization_success():
    """Test initialization of AZConfig with all parameters set."""
    client_params = AZClientConfigParams()
    config_params = AZConfigParams()
    client_config = AZClientConfig(**dict(client_params))

    c = AZConfig(client_config=client_config, **dict(config_params))

    # AZConfig Assertions
    assert c.client_config == client_config
    assert c.auth_mode == config_params.auth_mode
    assert c.connection_string == config_params.connection_string
    assert c.account_url == None
    assert c.container_name == config_params.container_name
    assert c.managed_identity_client_id == None
    assert c.managed_identity_resource_id == None
    assert c.create_container_if_missing == config_params.create_container_if_missing
    assert (
        c.remote_path == config_params.remote_path.strip("/")
        if config_params.remote_path != "/"
        else config_params.remote_path
    )
    assert c.max_concurrency == config_params.max_concurrency

    # AZClientConfig Assertions
    assert c.client_config.api_version == client_params.api_version
    assert c.client_config.secondary_hostname == client_params.secondary_hostname
    assert c.client_config.max_block_size == client_params.max_block_size
    assert c.client_config.max_single_put_size == client_params.max_single_put_size
    assert c.client_config.min_large_block_upload_threshold == client_params.min_large_block_upload_threshold
    assert c.client_config.use_byte_buffer == client_params.use_byte_buffer
    assert c.client_config.max_page_size == client_params.max_page_size
    assert c.client_config.max_single_get_size == client_params.max_single_get_size
    assert c.client_config.max_chunk_get_size == client_params.max_chunk_get_size
    assert c.client_config.audience == client_params.audience
    assert c.client_config.connection_timeout == client_params.connection_timeout


def test_initialization_success_defaults():
    """Test initialization of AZConfig with only required parameters set and ensure default values."""
    client_params = AZClientConfigParams(only_required=True)
    config_params = AZConfigParams(only_required=True)
    client_config = AZClientConfig(**dict(client_params))

    c = AZConfig(client_config=client_config, **dict(config_params))

    # AZConfig Assertions
    assert c.client_config == client_config
    assert c.auth_mode == AUTH_MODE_CONNECTION_STRING
    assert c.connection_string == config_params.connection_string
    assert c.account_url == None
    assert c.container_name == config_params.container_name
    assert c.managed_identity_client_id == None
    assert c.managed_identity_resource_id == None
    assert c.create_container_if_missing == True
    assert c.remote_path == "/"
    assert c.max_concurrency == 1

    # AZClientConfig Assertions
    assert c.client_config.api_version == None
    assert c.client_config.secondary_hostname == None
    assert c.client_config.max_block_size == 4 * 1024 * 1024  # 4MB
    assert c.client_config.max_single_put_size == 64 * 1024 * 1024  # 64MB
    assert c.client_config.min_large_block_upload_threshold == (4 * 1024 * 1024) + 1  # 4MB + 1
    assert c.client_config.use_byte_buffer == False
    assert c.client_config.max_page_size == 4 * 1024 * 1024  # 4MB
    assert c.client_config.max_single_get_size == 32 * 1024 * 1024  # 32MB
    assert c.client_config.max_chunk_get_size == 4 * 1024 * 1024  # 4MB
    assert c.client_config.audience == None
    assert c.client_config.connection_timeout == 20


def test_initialization_success_managed_identity():
    """Test initialization of AZConfig for managed identity auth."""
    client_params = AZClientConfigParams(only_required=True)
    config_params = AZConfigParams(
        only_required=True,
        auth_mode=AUTH_MODE_MANAGED_IDENTITY,
        managed_identity_client_id="test-client-id",
        create_container_if_missing=False,
    )
    client_config = AZClientConfig(**dict(client_params))

    c = AZConfig(client_config=client_config, **dict(config_params))

    assert c.client_config == client_config
    assert c.auth_mode == AUTH_MODE_MANAGED_IDENTITY
    assert c.connection_string == None
    assert c.account_url == config_params.account_url
    assert c.container_name == config_params.container_name
    assert c.managed_identity_client_id == "test-client-id"
    assert c.managed_identity_resource_id == None
    assert c.create_container_if_missing == False
    assert c.remote_path == "/"
    assert c.max_concurrency == 1


def test_initialization_success_managed_identity_resource_id():
    """Test initialization of AZConfig for managed identity auth with resource_id."""
    resource_id = (
        "/subscriptions/00000000-0000-0000-0000-000000000000"
        "/resourceGroups/example-rg"
        "/providers/Microsoft.ManagedIdentity/userAssignedIdentities"
        "/example-mi"
    )
    client_params = AZClientConfigParams(only_required=True)
    config_params = AZConfigParams(
        only_required=True,
        auth_mode=AUTH_MODE_MANAGED_IDENTITY,
        managed_identity_resource_id=resource_id,
        create_container_if_missing=False,
    )
    client_config = AZClientConfig(**dict(client_params))

    c = AZConfig(client_config=client_config, **dict(config_params))

    assert c.auth_mode == AUTH_MODE_MANAGED_IDENTITY
    assert c.managed_identity_client_id == None
    assert c.managed_identity_resource_id == resource_id


def test_invalid_params():
    """Test initialization of AZConfig with invalid parameters."""

    # Invalidate AZConfig
    with pytest.raises(ValueError):  # Invalid client_config
        AZConfig(client_config={}, container_name="test_container", connection_string="test_connection_string")
    with pytest.raises(ValueError):  # Invalid connection_string
        AZConfig(client_config=AZClientConfig(), container_name="test_container", connection_string=123)
    with pytest.raises(ValueError):  # Missing connection_string for connection string auth
        AZConfig(client_config=AZClientConfig(), container_name="test_container")
    with pytest.raises(ValueError):  # Invalid auth_mode
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            auth_mode="service_principal",
            connection_string="test_connection_string",
        )
    with pytest.raises(ValueError):  # Missing account_url for managed identity auth
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            auth_mode=AUTH_MODE_MANAGED_IDENTITY,
        )
    with pytest.raises(ValueError):  # Invalid account_url
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            auth_mode=AUTH_MODE_MANAGED_IDENTITY,
            account_url=123,
        )
    with pytest.raises(ValueError):  # Invalid managed_identity_client_id
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            auth_mode=AUTH_MODE_MANAGED_IDENTITY,
            account_url="https://account-name.blob.core.windows.net",
            managed_identity_client_id=123,
        )
    with pytest.raises(ValueError):  # Invalid managed_identity_resource_id
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            auth_mode=AUTH_MODE_MANAGED_IDENTITY,
            account_url="https://account-name.blob.core.windows.net",
            managed_identity_resource_id=123,
        )
    with pytest.raises(ValueError, match="mutually exclusive"):  # Both client_id and resource_id set
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            auth_mode=AUTH_MODE_MANAGED_IDENTITY,
            account_url="https://account-name.blob.core.windows.net",
            managed_identity_client_id="test-client-id",
            managed_identity_resource_id="/subscriptions/abc/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/name",
        )
    with pytest.raises(ValueError):  # Invalid remote_path
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            connection_string="test_connection_string",
            remote_path=1,
        )
    with pytest.raises(ValueError):  # Invalid container_name
        AZConfig(client_config=AZClientConfig(), container_name=1, connection_string="test_connection_string")
    with pytest.raises(ValueError):  # Invalid create_container_if_missing
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            connection_string="test_connection_string",
            create_container_if_missing="true",
        )
    with pytest.raises(ValueError):  # Invalid max_concurrency
        AZConfig(
            client_config=AZClientConfig(),
            container_name="test_container",
            connection_string="test_connection_string",
            max_concurrency="1",
        )

    # Invalidate AZClientConfig
    with pytest.raises(ValueError):  # Invalid api_version
        AZClientConfig(api_version=123)
    with pytest.raises(ValueError):  # Invalid secondary_hostname
        AZClientConfig(secondary_hostname=123)
    with pytest.raises(ValueError):  # Invalid max_block_size
        AZClientConfig(max_block_size="123")
    with pytest.raises(ValueError):  # Invalid max_single_put_size
        AZClientConfig(max_single_put_size="123")
    with pytest.raises(ValueError):  # Invalid min_large_block_upload_threshold
        AZClientConfig(min_large_block_upload_threshold="123")
    with pytest.raises(ValueError):  # Invalid use_byte_buffer
        AZClientConfig(use_byte_buffer="123")
    with pytest.raises(ValueError):  # Invalid max_page_size
        AZClientConfig(max_page_size="123")
    with pytest.raises(ValueError):  # Invalid max_single_get_size
        AZClientConfig(max_single_get_size="123")
    with pytest.raises(ValueError):  # Invalid max_chunk_get_size
        AZClientConfig(max_chunk_get_size="123")
    with pytest.raises(ValueError):  # Invalid audience
        AZClientConfig(audience=123)
    with pytest.raises(ValueError):  # Invalid connection_timeout
        AZClientConfig(connection_timeout="123")


def test_drop_empty_dicts_some_undefined():
    """Test drop_empty_dict_factory helper function."""

    client_config = AZClientConfig(**dict(AZClientConfigParams(only_required=True)))

    # Convert to dict and drop attributes with None values
    client_config_dict = asdict(client_config, dict_factory=drop_empty_dict_factory)

    # Assert that the dict does not contain any None values
    assert "api_version" not in client_config_dict
    assert "secondary_hostname" not in client_config_dict
    assert "audience" not in client_config_dict


def test_drop_empty_dicts_all_defined():
    """Test drop_empty_dict_factory helper function doesn't drop any attributes when all are defined."""

    client_config = AZClientConfig(**dict(AZClientConfigParams()))

    # Convert to dict and drop attributes with None values
    client_config_dict_drop_empty = asdict(client_config, dict_factory=drop_empty_dict_factory)

    # Convert to dict
    client_config_dict = asdict(client_config)

    # Assert that the dicts are the same
    assert client_config_dict == client_config_dict_drop_empty
