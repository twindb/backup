import collections
from unittest.mock import MagicMock, patch

from azure.storage.blob import ContainerClient

import twindb_backup.destination.az as az


class AZParams(collections.Mapping):
    def __init__(self, only_required=False) -> None:
        self.container_name = "test_container"
        self.connection_string = "DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;EndpointSuffix=core.windows.net"

        if not only_required:
            self.hostname = "test_host"
            self.chunk_size = 123
            self.remote_path = "/himom"

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]


class AZConfigParams(collections.Mapping):
    def __init__(self, only_required=False) -> None:
        self.connection_string = "test_connection_string"
        self.container_name = "test_container"

        if not only_required:
            self.chunk_size = 123
            self.remote_path = "/himom"

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]


def mocked_az():
    with patch("twindb_backup.destination.az.AZ._connect") as mc:
        mc.return_value = MagicMock(spec=ContainerClient)
        p = AZParams()
        c = az.AZ(**dict(p))

    return c
