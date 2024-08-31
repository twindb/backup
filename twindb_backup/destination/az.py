# -*- coding: utf-8 -*-
"""
Module for Azure destination.
"""
import builtins
import os
import socket
import typing as T
from contextlib import contextmanager
from multiprocessing import Process

import azure.core.exceptions as ae
from azure.storage.blob import ContainerClient

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import FileNotFound


class AZ(BaseDestination):
    """Azure Blob Storage Destination class"""

    def __init__(
        self,
        container_name: str,
        connection_string: str,
        hostname: str = socket.gethostname(),
        chunk_size: int = 4 * 1024 * 1024,  # TODO: Add support for chunk size
        remote_path: str = "/",
    ) -> None:
        """Creates an instance of the Azure Blob Storage Destination class,
          initializes the ContainerClient and validates the connection settings

        Args:
            container_name (str): Name of the container in the Azure storage account
            connection_string (str): Connection string for the Azure storage account
            hostname (str, optional): Hostname of the host performing the backup. Defaults to socket.gethostname().
            chunk_size (int, optional): Size in bytes for read/write streams. Defaults to 4*1024*1024.
            remote_path (str, optional): Remote base path in the container to store backups. Defaults to "/".

        Raises:
            err: Raises an error if the client cannot be initialized
        """

        self._container_name = container_name
        self._connection_string = connection_string
        self._hostname = hostname
        self._chunk_size = chunk_size
        self._remote_path = remote_path
        super(AZ, self).__init__(self._remote_path)

        self._container_client = self._connect()

    """HELPER FUNCTIONS
    """

    def _connect(self) -> ContainerClient:
        """Connects to an Azure Storage Account and initializes a ContainerClient,
        ensures the container exists, creating one if not found

        Raises:
            err: Returns an error if the connection string is invalid or we failed to validate the container

        Returns:
            ContainerClient: An initialized ContainerClient
        """

        client: ContainerClient = None

        # Create the container client - validates connection string format
        try:
            client = ContainerClient.from_connection_string(self._connection_string, self._container_name)
        except builtins.ValueError as err:
            LOG.error(f"Failed to create Azure Client. Error: {type(err).__name__}, Reason: {err}")
            raise err

        # Check if the container exists, if not, create it
        try:
            if not client.exists():
                client.create_container()
        except builtins.Exception as err:
            LOG.error(f"Failed to validate or create container. Error: {type(err).__name__}, Reason: {err}")
            raise err

        return client

    def render_path(self, path: str) -> str:
        """Renders the absolute path for the Azure Blob Storage Destination

        Returns:
            str: Absolute path to the blob in the container
        """
        return f"{self._remote_path}/{path}"

    def _download_to_pipe(self, blob_key: str, pipe_in: int, pipe_out: int) -> None:
        """Downloads a blob from Azure Blob Storage and writes it to a pipe

        Args:
            blob_key (str): The path to the blob in the container
            pipe_in (int): The pipe to read the blob content from, closed in child process.
            pipe_out (int): The pipe to write the blob content to, closed in parent process.
        """
        os.close(pipe_in)
        with os.fdopen(pipe_out, "wb") as pipe_out_file:
            try:
                self._container_client.download_blob(blob_key).readinto(pipe_out_file)
            except builtins.Exception as err:
                LOG.error(f"Failed to download blob {blob_key}. Error: {type(err).__name__}, Reason: {err}")
                raise err

    """BaseDestination ABSTRACT METHODS IMPLEMENTATION
    """

    def delete(self, path: str) -> None:
        """Deletes a blob from the Azure storage account's container

        Args:
            path (str): Relative path to the blob in the container to delete

        Raises:
            err: Raises an error if the blob failed to be deleted
        """
        LOG.debug(f"Attempting to delete blob: {self.render_path(path)}")
        try:
            self._container_client.delete_blob(self.render_path(path))
        except builtins.Exception as err:
            LOG.error(f"Failed to delete blob {self.render_path(path)}. Error: {type(err).__name__}, Reason: {err}")
            raise err

    @contextmanager
    def get_stream(self, copy):
        """Streams a blob from Azure Blob Storage into a pipe

        Args:
            copy (BaseCopy): A copy object to stream from Azure

        Yields:
            T.Generator(T.BinaryIO): A generator that yields a stream of the blob's content
        """

        LOG.debug(f"Attempting to stream blob: {self.render_path(copy.key)}")
        pipe_in, pipe_out = os.pipe()

        proc = Process(target=self._download_to_pipe, args=(self.render_path(copy.key), pipe_in, pipe_out))
        proc.start()
        os.close(pipe_out)
        try:
            with os.fdopen(pipe_in, "rb") as pipe_in_file:
                yield pipe_in_file
        finally:
            proc.join()
            if proc.exitcode != 0:
                LOG.error(f"Failed to stream blob {self.render_path(copy.key)}")
                raise builtins.Exception(f"Failed to stream blob {self.render_path(copy.key)}")

    def read(self, filepath: str) -> bytes:
        """Read content of a file path from Azure Blob Storage

        Args:
            filepath (str): Relative path to a blob in the container

        Raises:
            err: Raises an error if the blob failed to be read or it does not exist

        Returns:
            bytes: Content of the blob
        """
        LOG.debug(f"Attempting to read blob: {self.render_path(filepath)}")
        try:
            return self._container_client.download_blob(self.render_path(filepath), encoding="utf-8").read()
        except ae.ResourceNotFoundError as err:
            LOG.debug(f"File {self.render_path(filepath)} does not exist in container {self._container_name}")
            raise FileNotFound(f"File {self.render_path(filepath)} does not exist in container {self._container_name}")
        except builtins.Exception as err:
            LOG.error(f"Failed to read blob {self.render_path(filepath)}. Error: {type(err).__name__}, Reason: {err}")
            raise err

    def save(self, handler: T.BinaryIO, filepath: str) -> None:
        """Save a stream given as handler to filepath in Azure Blob Storage

        Args:
            handler (T.BinaryIO): Incoming stream
            filepath (str): Relative path to a blob in the container

        Raises:
            err: Raises an error if the blob failed to be written
        """

        LOG.debug(f"Attempting to save blob: {self.render_path(filepath)}")
        with handler as file_obj:
            try:
                self._container_client.upload_blob(self.render_path(filepath), file_obj)
            except builtins.Exception as err:
                LOG.error(f"Failed to upload blob or it already exists. Error {type(err).__name__}, Reason: {err}")
                raise err

    def write(self, content: str, filepath: str) -> None:
        """Write content to filepath in Azure Blob Storage

        Args:
            content (str): Content to write to blob
            filepath (str): Relative path to a blob in the container

        Raises:
            err: Raises an error if the blob failed to be written
        """

        LOG.debug(f"Attempting to write blob: {self.render_path(filepath)}")
        try:
            self._container_client.upload_blob(self.render_path(filepath), content, overwrite=True)
        except builtins.Exception as err:
            LOG.error(f"Failed to upload or overwrite blob. Error {type(err).__name__}, Reason: {err}")
            raise err

    def _list_files(self, prefix: str = "", recursive: bool = False, files_only: bool = False) -> T.List[str]:
        """List files in the Azure Blob Storage container

        Args:
            prefix (str, optional): Filters blobs by a given prefix. Defaults to "".
            recursive (bool, optional): Not supported. Defaults to False.
            files_only (bool, optional): Excludes directories when true,
                otherwise includes files and directories. Defaults to False.
        """
        LOG.debug(
            f"""Listing files in container {self._container_name} with prefix={prefix},
              recursive={recursive}, files_only={files_only}"""
        )

        try:
            blobs = self._container_client.list_blobs(name_starts_with=prefix, include=["metadata"])
        except builtins.Exception as err:
            LOG.error(
                f"Failed to list files in container {self._container_name}. Error: {type(err).__name__}, Reason: {err}"
            )
            raise err

        return [
            blob.name
            for blob in blobs
            if not files_only
            or not (bool(blob.get("metadata")) and blob.get("metadata", {}).get("hdi_isfolder") == "true")
        ]
