"""Azure Blob Storage destination configuration"""

import typing as t
from dataclasses import dataclass

AUTH_MODE_CONNECTION_STRING = "connection_string"
AUTH_MODE_MANAGED_IDENTITY = "managed_identity"
SUPPORTED_AUTH_MODES = (AUTH_MODE_CONNECTION_STRING, AUTH_MODE_MANAGED_IDENTITY)


# Parameters taken from:
# https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#keyword-only-parameters
@dataclass
class AZClientConfig:
    """Azure Blob Container Client Configuration

    Attributes:
        api_version (str, optional): The version of the Azure Storage API to use. Defaults to None.
        secondary_hostname (str, optional): The secondary hostname to use for the storage account. Defaults to None.
        max_block_size (int): The maximum size of a block in bytes. Defaults to 4MB.
        max_single_put_size (int): The maximum size of a single put operation in bytes. Defaults to 64MB.
        min_large_block_upload_threshold (int): The minimum size threshold for large block uploads in bytes.
            Defaults to 4MB + 1.
        use_byte_buffer (bool): Whether to use a byte buffer for uploads. Defaults to False.
        max_page_size (int): The maximum size of a page in bytes. Defaults to 4MB.
        max_single_get_size (int): The maximum size of a single get operation in bytes. Defaults to 32MB.
        max_chunk_get_size (int): The maximum size of a chunk in bytes for get operations. Defaults to 4MB.
        audience (str, optional): The audience for the Azure Storage account. Defaults to None.
        connection_timeout (int): The connection timeout in seconds. Defaults to 20.
    """

    api_version: t.Optional[str] = None
    secondary_hostname: t.Optional[str] = None
    max_block_size: int = 4 * 1024 * 1024  # 4MB
    max_single_put_size: int = 64 * 1024 * 1024  # 64MB
    min_large_block_upload_threshold: int = (4 * 1024 * 1024) + 1  # 4MB + 1
    use_byte_buffer: bool = False
    max_page_size: int = 4 * 1024 * 1024  # 4MB
    max_single_get_size: int = 32 * 1024 * 1024  # 32MB
    max_chunk_get_size: int = 4 * 1024 * 1024  # 4MB
    audience: t.Optional[str] = None
    connection_timeout: int = 20

    def validate(self) -> None:
        """Validates the configuration parameters for the Azure destination.

        Raises:
            ValueError: Raises a ValueError if the type hint or value is incorrect for any of the parameters.
        """

        if self.api_version is not None and not isinstance(self.api_version, str):
            raise ValueError("api_version must be a string or undefined")
        if self.secondary_hostname is not None and not isinstance(self.secondary_hostname, str):
            raise ValueError("secondary_hostname must be a string or undefined")
        if not isinstance(self.max_block_size, int) or self.max_block_size <= 0:
            raise ValueError("max_block_size must be a positive integer")
        if not isinstance(self.max_single_put_size, int) or self.max_single_put_size <= 0:
            raise ValueError("max_single_put_size must be a positive integer")
        if not isinstance(self.min_large_block_upload_threshold, int) or self.min_large_block_upload_threshold <= 0:
            raise ValueError("min_large_block_upload_threshold must be a positive integer")
        if not isinstance(self.use_byte_buffer, bool):
            raise ValueError("use_byte_buffer must be a boolean")
        if not isinstance(self.max_page_size, int) or self.max_page_size <= 0:
            raise ValueError("max_page_size must be a positive integer")
        if not isinstance(self.max_single_get_size, int) or self.max_single_get_size <= 0:
            raise ValueError("max_single_get_size must be a positive integer")
        if not isinstance(self.max_chunk_get_size, int) or self.max_chunk_get_size <= 0:
            raise ValueError("max_chunk_get_size must be a positive integer")
        if self.audience is not None and not isinstance(self.audience, str):
            raise ValueError("audience must be a string or undefined")
        if not isinstance(self.connection_timeout, int) or self.connection_timeout <= 0:
            raise ValueError("connection_timeout must be a positive integer")

    def __post_init__(self) -> None:
        self.validate()


@dataclass
class AZConfig:
    """Azure Blob Storage Configuration

    Attributes:
        client_config (AZClientConfig): Configuration for the Azure Blob Container Client.
        container_name (str): Name of the container in the Azure storage account.
        auth_mode (str): Azure authentication mode. Defaults to connection_string.
        connection_string (str, optional): Connection string for the Azure storage account.
        account_url (str, optional): Blob service account URL used for managed identity authentication.
        managed_identity_client_id (str, optional): User-assigned managed identity client ID. Mutually
            exclusive with managed_identity_resource_id.
        managed_identity_resource_id (str, optional): ARM resource ID of a user-assigned managed identity.
            Preferred over managed_identity_client_id when multiple UAMIs are attached to the VM because
            the resource ID is deterministic from naming and doesn't require reading the UAMI's GUID out
            of Terraform. Mutually exclusive with managed_identity_client_id.
        create_container_if_missing (bool, optional): Create the container if it does not exist.
            Defaults to True.
        remote_path (str, optional): Remote base path in the container to store backups. Defaults to "/".
        max_concurrency (int, optional): Maximum number of concurrent requests to the Azure Storage service.
            Defaults to 1.
    """

    client_config: AZClientConfig
    container_name: str
    connection_string: t.Optional[str] = None
    account_url: t.Optional[str] = None
    auth_mode: str = AUTH_MODE_CONNECTION_STRING
    managed_identity_client_id: t.Optional[str] = None
    managed_identity_resource_id: t.Optional[str] = None
    create_container_if_missing: bool = True
    remote_path: str = "/"
    max_concurrency: int = 1

    def validate(self) -> None:
        """Validates the configuration parameters for the Azure destination.

        Raises:
            ValueError: Raises a ValueError if the type hint or value is incorrect for any of the parameters.
        """

        if not isinstance(self.client_config, AZClientConfig):
            raise ValueError("client_config must be an instance of AZClientConfig")
        if not isinstance(self.container_name, str):
            raise ValueError("container_name must be a string")
        if self.connection_string is not None and not isinstance(self.connection_string, str):
            raise ValueError("connection_string must be a string or undefined")
        if self.account_url is not None and not isinstance(self.account_url, str):
            raise ValueError("account_url must be a string or undefined")
        if not isinstance(self.auth_mode, str) or self.auth_mode not in SUPPORTED_AUTH_MODES:
            raise ValueError(f"auth_mode must be one of: {', '.join(SUPPORTED_AUTH_MODES)}")
        if self.managed_identity_client_id is not None and not isinstance(self.managed_identity_client_id, str):
            raise ValueError("managed_identity_client_id must be a string or undefined")
        if self.managed_identity_resource_id is not None and not isinstance(self.managed_identity_resource_id, str):
            raise ValueError("managed_identity_resource_id must be a string or undefined")
        if self.managed_identity_client_id and self.managed_identity_resource_id:
            raise ValueError(
                "managed_identity_client_id and managed_identity_resource_id are mutually exclusive; set at most one"
            )
        if not isinstance(self.create_container_if_missing, bool):
            raise ValueError("create_container_if_missing must be a boolean")
        if not isinstance(self.remote_path, str):
            raise ValueError("remote_path must be a string")
        if not isinstance(self.max_concurrency, int) or self.max_concurrency <= 0:
            raise ValueError("max_concurrency must be a positive integer")
        if self.auth_mode == AUTH_MODE_CONNECTION_STRING and not self.connection_string:
            raise ValueError("connection_string is required when auth_mode=connection_string")
        if self.auth_mode == AUTH_MODE_MANAGED_IDENTITY and not self.account_url:
            raise ValueError("account_url is required when auth_mode=managed_identity")

    def __post_init__(self) -> None:
        self.validate()
        self.remote_path = self.remote_path.strip("/") if self.remote_path != "/" else self.remote_path


def drop_empty_dict_factory(d):
    """Drop empty values from a dictionary"""
    return {k: v for k, v in d if v is not None}
