"""Azure Blob Storage destination configuration"""


class AZConfig:
    """Azure Blob Storage Configuration."""

    def __init__(self, connection_string, container_name, chunk_size=1024 * 1024 * 4):  # 4MiB

        self._connection_string = connection_string
        self._container_name = container_name
        self._chunk_size = chunk_size

    @property
    def connection_string(self):
        """CONNECTION_STRING"""
        return self._connection_string

    @property
    def container_name(self):
        """CONTAINER_NAME"""
        return self._container_name

    @property
    def chunk_size(self):
        """CHUNK_SIZE"""
        return self._chunk_size
