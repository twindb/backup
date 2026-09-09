from dataclasses import asdict
from unittest.mock import MagicMock, patch

import azure.core.exceptions as ae
import pytest
from azure.storage.blob import ContainerClient

import twindb_backup.destination.az as az
from twindb_backup.configuration.destinations.az import (
    AUTH_MODE_MANAGED_IDENTITY,
    AZClientConfig,
    AZConfig,
    drop_empty_dict_factory,
)

from .util import AZClientConfigParams, AZConfigParams


def test_init_param():
    """Test initialization of AZ with all parameters set, mocking the _connect method."""
    with patch("twindb_backup.destination.az.AZ._connect") as mc:
        mc.return_value = MagicMock(spec=ContainerClient)

        client_params = AZClientConfigParams()
        config_params = AZConfigParams()
        client_config = AZClientConfig(**dict(client_params))
        config = AZConfig(client_config=client_config, **dict(config_params))

        c = az.AZ(config=config)

        assert isinstance(c.container_client, ContainerClient)
        az.AZ._connect.assert_called_once()


def test_init_param_defaults():
    """Test initialization of AZ with only required parameters set, ensuring default values, mocking the _connect method."""
    with patch("twindb_backup.destination.az.AZ._connect") as mc:
        mc.return_value = MagicMock(spec=ContainerClient)

        client_params = AZClientConfigParams()
        config_params = AZConfigParams()
        client_config = AZClientConfig(**dict(client_params))
        config = AZConfig(client_config=client_config, **dict(config_params))

        c = az.AZ(config=config)

        assert isinstance(c.container_client, ContainerClient)
        az.AZ._connect.assert_called_once()


def test_init_conn_string_valid():
    """Test initialization of AZ with valid connection string."""
    with patch("twindb_backup.destination.az.ContainerClient.exists") as mc:
        mc.return_value = True

        client_params = AZClientConfigParams()
        config_params = AZConfigParams()
        client_config = AZClientConfig(**dict(client_params))
        config = AZConfig(client_config=client_config, **dict(config_params))

        c = az.AZ(config=config)

        az.ContainerClient.exists.assert_called_once()
        assert isinstance(c.container_client, ContainerClient)


def test_init_conn_string_invalid():
    """Test initialization of AZ with invalid connection string, expecting ValueError."""
    with patch("twindb_backup.destination.az.ContainerClient.exists") as mc:
        mc.return_value = True

        client_params = AZClientConfigParams()
        config_params = AZConfigParams()
        client_config = AZClientConfig(**dict(client_params))
        config = AZConfig(client_config=client_config, **dict(config_params))
        config.connection_string = "invalid_connection_string"

        with pytest.raises(ValueError, match="Connection string is either blank or malformed."):
            _ = az.AZ(config=config)


def test_init_container_not_exists():
    """Test initialization of AZ with container not existing, mocking the create_container method."""
    with patch("twindb_backup.destination.az.ContainerClient.exists") as mc:
        mc.return_value = False
        with patch("twindb_backup.destination.az.ContainerClient.create_container") as mc_create_container:
            mc_create_container.return_value = MagicMock(spec=ContainerClient)

            client_params = AZClientConfigParams()
            config_params = AZConfigParams()
            client_config = AZClientConfig(**dict(client_params))
            config = AZConfig(client_config=client_config, **dict(config_params))

            c = az.AZ(config=config)

            az.ContainerClient.exists.assert_called_once()
            az.ContainerClient.create_container.assert_called_once()
            assert isinstance(c.container_client, ContainerClient)


def test_init_container_create_fails():
    """Test initialization of AZ with container not existing, fails to create container, re-raising error."""
    with patch("twindb_backup.destination.az.ContainerClient.exists") as mc:
        mc.return_value = False
        with patch("twindb_backup.destination.az.ContainerClient.create_container") as mc_create_container:
            mc_create_container.side_effect = ae.HttpResponseError()

            client_params = AZClientConfigParams()
            config_params = AZConfigParams()
            client_config = AZClientConfig(**dict(client_params))
            config = AZConfig(client_config=client_config, **dict(config_params))

            with pytest.raises(Exception):
                c = az.AZ(config=config)

                az.ContainerClient.exists.assert_called_once()
                az.ContainerClient.create_container.assert_called_once()
                assert isinstance(c.container_client, ContainerClient)


def test_init_container_create_disabled():
    """Test initialization of AZ with missing container and create disabled."""
    with patch("twindb_backup.destination.az.ContainerClient.exists") as mc:
        mc.return_value = False
        with patch("twindb_backup.destination.az.ContainerClient.create_container") as mc_create_container:
            client_params = AZClientConfigParams()
            config_params = AZConfigParams(create_container_if_missing=False)
            client_config = AZClientConfig(**dict(client_params))
            config = AZConfig(client_config=client_config, **dict(config_params))

            with pytest.raises(ValueError, match="create_container_if_missing is disabled"):
                _ = az.AZ(config=config)

            az.ContainerClient.exists.assert_called_once()
            az.ContainerClient.create_container.assert_not_called()


def test_init_success():
    """Test initialization of AZ with existing container, mocking the from_connection_string method."""
    with patch("twindb_backup.destination.az.ContainerClient.from_connection_string") as mc:
        mc.return_value = MagicMock(spec=ContainerClient)

        client_params = AZClientConfigParams()
        config_params = AZConfigParams()
        client_config = AZClientConfig(**dict(client_params))
        config = AZConfig(client_config=client_config, **dict(config_params))

        c = az.AZ(config=config)

        az.ContainerClient.from_connection_string.assert_called_once_with(
            conn_str=config.connection_string,
            container_name=config.container_name,
            **asdict(config.client_config, dict_factory=drop_empty_dict_factory)
        )
        mc.return_value.exists.assert_called_once()
        mc.return_value.create_container.assert_not_called()
        assert isinstance(c.container_client, ContainerClient)


def test_init_managed_identity_success():
    """Test initialization of AZ with managed identity auth (no client/resource id → DefaultAzureCredential)."""
    with patch("twindb_backup.destination.az.DefaultAzureCredential") as mc_default_credential:
        credential = MagicMock(name="credential")
        mc_default_credential.return_value = credential

        with patch("twindb_backup.destination.az.ManagedIdentityCredential") as mc_mi_credential:
            with patch("twindb_backup.destination.az.ContainerClient") as mc_container_client:
                client = MagicMock(spec=ContainerClient)
                client.exists.return_value = True
                mc_container_client.return_value = client

                client_params = AZClientConfigParams()
                config_params = AZConfigParams(only_required=True, auth_mode=AUTH_MODE_MANAGED_IDENTITY)
                client_config = AZClientConfig(**dict(client_params))
                config = AZConfig(client_config=client_config, **dict(config_params))

                c = az.AZ(config=config)

                mc_default_credential.assert_called_once_with()
                mc_mi_credential.assert_not_called()
                mc_container_client.assert_called_once_with(
                    account_url=config.account_url,
                    container_name=config.container_name,
                    credential=credential,
                    **asdict(config.client_config, dict_factory=drop_empty_dict_factory),
                )
                client.exists.assert_called_once()
                client.create_container.assert_not_called()
                assert c.container_client == client


def test_init_managed_identity_user_assigned_client_id():
    """Test initialization of AZ with a user-assigned managed identity by client_id."""
    with patch("twindb_backup.destination.az.ManagedIdentityCredential") as mc_mi_credential:
        credential = MagicMock(name="credential")
        mc_mi_credential.return_value = credential

        with patch("twindb_backup.destination.az.DefaultAzureCredential") as mc_default_credential:
            with patch("twindb_backup.destination.az.ContainerClient") as mc_container_client:
                client = MagicMock(spec=ContainerClient)
                client.exists.return_value = True
                mc_container_client.return_value = client

                client_params = AZClientConfigParams(only_required=True)
                config_params = AZConfigParams(
                    only_required=True,
                    auth_mode=AUTH_MODE_MANAGED_IDENTITY,
                    managed_identity_client_id="test-client-id",
                )
                client_config = AZClientConfig(**dict(client_params))
                config = AZConfig(client_config=client_config, **dict(config_params))

                _ = az.AZ(config=config)

                mc_mi_credential.assert_called_once_with(client_id="test-client-id")
                mc_default_credential.assert_not_called()


def test_init_managed_identity_user_assigned_resource_id():
    """Test initialization of AZ with a user-assigned managed identity by resource_id."""
    resource_id = (
        "/subscriptions/00000000-0000-0000-0000-000000000000"
        "/resourceGroups/example-rg"
        "/providers/Microsoft.ManagedIdentity/userAssignedIdentities"
        "/example-mi"
    )
    with patch("twindb_backup.destination.az.ManagedIdentityCredential") as mc_mi_credential:
        credential = MagicMock(name="credential")
        mc_mi_credential.return_value = credential

        with patch("twindb_backup.destination.az.DefaultAzureCredential") as mc_default_credential:
            with patch("twindb_backup.destination.az.ContainerClient") as mc_container_client:
                client = MagicMock(spec=ContainerClient)
                client.exists.return_value = True
                mc_container_client.return_value = client

                client_params = AZClientConfigParams(only_required=True)
                config_params = AZConfigParams(
                    only_required=True,
                    auth_mode=AUTH_MODE_MANAGED_IDENTITY,
                    managed_identity_resource_id=resource_id,
                )
                client_config = AZClientConfig(**dict(client_params))
                config = AZConfig(client_config=client_config, **dict(config_params))

                _ = az.AZ(config=config)

                mc_mi_credential.assert_called_once_with(identity_config={"resource_id": resource_id})
                mc_default_credential.assert_not_called()
