from collections.abc import Mapping
from unittest.mock import MagicMock, patch

from azure.storage.blob import ContainerClient

import twindb_backup.destination.az as az
from twindb_backup.configuration.destinations.az import (
    AUTH_MODE_CONNECTION_STRING,
    AUTH_MODE_MANAGED_IDENTITY,
    AZClientConfig,
    AZConfig,
)


class AZClientConfigParams(Mapping):
    def __init__(self, only_required=False) -> None:

        if not only_required:
            self.api_version = "2021-04-10"
            self.secondary_hostname = "secondary.example.com"
            self.max_block_size = 128 * 1024 * 1024  # 128MB
            self.max_single_put_size = 128 * 1024 * 1024  # 128MB
            self.min_large_block_upload_threshold = 128 * 1024 * 1024  # 128MB
            self.use_byte_buffer = False
            self.max_page_size = 128 * 1024 * 1024  # 128MB
            self.max_single_get_size = 128 * 1024 * 1024  # 128MB
            self.max_chunk_get_size = 128 * 1024 * 1024  # 128MB
            self.audience = "https://example.com"
            self.connection_timeout = 30

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]


class AZConfigParams(Mapping):
    def __init__(
        self,
        only_required=False,
        auth_mode=AUTH_MODE_CONNECTION_STRING,
        managed_identity_client_id=None,
        managed_identity_resource_id=None,
        create_container_if_missing=True,
    ) -> None:
        self.container_name = "test_container"
        self.auth_mode = auth_mode
        self.create_container_if_missing = create_container_if_missing

        if auth_mode == AUTH_MODE_CONNECTION_STRING:
            self.connection_string = (
                "DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;"
                "AccountKey=ACCOUNT_KEY;EndpointSuffix=core.windows.net"
            )
        elif auth_mode == AUTH_MODE_MANAGED_IDENTITY:
            self.account_url = "https://account-name.blob.core.windows.net"
            if managed_identity_client_id is not None:
                self.managed_identity_client_id = managed_identity_client_id
            if managed_identity_resource_id is not None:
                self.managed_identity_resource_id = managed_identity_resource_id

        if not only_required:
            self.remote_path = "/himom/"
            self.max_concurrency = 4

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]


def mocked_az():
    with patch("twindb_backup.destination.az.AZ._connect") as mc:
        mc.return_value = MagicMock(spec=ContainerClient)

        client_params = AZClientConfigParams()
        config_params = AZConfigParams()

        az_config = AZConfig(client_config=AZClientConfig(**dict(client_params)), **dict(config_params))

        c = az.AZ(config=az_config)

    return c
