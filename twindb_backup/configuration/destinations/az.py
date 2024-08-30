"""Azure Blob Storage destination configuration"""


class AZConfig:
    """Azure Blob Storage Configuration."""

    def __init__(
        self, connection_string: str, container_name: str, chunk_size: int = 1024 * 1024 * 4, remote_path: str = "/"
    ):

        self._connection_string = connection_string
        self._container_name = container_name
        self._chunk_size = chunk_size
        self._remote_path = remote_path

    @property
    def connection_string(self) -> str:
        """CONNECTION_STRING"""
        return self._connection_string

    @property
    def container_name(self) -> str:
        """CONTAINER_NAME"""
        return self._container_name

    @property
    def chunk_size(self) -> int:
        """CHUNK_SIZE"""
        return self._chunk_size

    @property
    def remote_path(self) -> str:
        """REMOTE_PATH"""
        return self._remote_path
