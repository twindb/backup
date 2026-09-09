# -*- coding: utf-8 -*-
"""
Module for Azure destination.
"""
import builtins
import os
import threading
import typing as t
from contextlib import contextmanager
from dataclasses import asdict
from multiprocessing import Process

import azure.core.exceptions as ae
from azure.storage.blob import BlobLeaseClient, ContainerClient

from twindb_backup import LOG
from twindb_backup.configuration.destinations.az import (
    AUTH_MODE_MANAGED_IDENTITY,
    AZConfig,
    drop_empty_dict_factory,
)
from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.destination.base_destination import BaseDestination, ClusterLock
from twindb_backup.destination.exceptions import FileNotFound

try:
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
except ImportError:  # pragma: no cover - dependency is optional until managed identity is configured
    DefaultAzureCredential = None
    ManagedIdentityCredential = None

_CLUSTER_LOCK_BLOB = ".twindb-backup/cluster.lock"
_CLUSTER_LOCK_MIN_TTL = 15
_CLUSTER_LOCK_MAX_TTL = 60


class _LeaseRenewer(threading.Thread):
    """Background thread that keeps an Azure blob lease alive.

    Azure caps blob leases at 60 seconds, so long-running backups need
    to renew periodically. The renewer sleeps ``ttl / 2`` between
    renewals and stops quietly when :meth:`stop` is called or when a
    renewal fails (the main thread will discover the lost lease when
    it tries to release it).
    """

    def __init__(self, lease, ttl):
        super(_LeaseRenewer, self).__init__(daemon=True, name="az-lease-renewer")
        self._lease = lease
        self._stop = threading.Event()
        self._interval = max(5, ttl // 2)

    def run(self):
        while not self._stop.wait(self._interval):
            try:
                self._lease.renew()
                LOG.debug("Renewed Azure cluster-lock lease")
            except builtins.Exception as err:  # pragma: no cover - best-effort
                LOG.warning("Failed to renew Azure cluster-lock lease: %s", err)
                return

    def stop(self):
        self._stop.set()


class AZ(BaseDestination):
    """Azure Blob Storage Destination class"""

    def __init__(self, config: AZConfig) -> None:
        """Creates an instance of the Azure Blob Storage Destination class,
          initializes the ContainerClient and validates the connection settings

        Args:
            config (AZConfig): Azure Blob Storage Configuration

        Raises:
            err: Raises an error if the client cannot be initialized
        """
        self.config = config
        self._credential = None

        super(AZ, self).__init__(self.config.remote_path)

        self.container_client = self._connect()

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
        client = self._create_container_client()

        # Check if the container exists, if not, create it
        try:
            if not client.exists():
                if self.config.create_container_if_missing:
                    client.create_container()
                else:
                    raise builtins.ValueError(
                        f"Container {self.config.container_name} does not exist and "
                        "create_container_if_missing is disabled"
                    )
        except builtins.Exception as err:
            LOG.error(f"Failed to validate or create container. Error: {type(err).__name__}, Reason: {err}")
            raise err

        return client

    def _create_container_client(self) -> ContainerClient:
        client_kwargs = asdict(self.config.client_config, dict_factory=drop_empty_dict_factory)

        try:
            if self.config.auth_mode == AUTH_MODE_MANAGED_IDENTITY:
                self._credential = self._build_managed_identity_credential()
                return ContainerClient(
                    account_url=self.config.account_url,
                    container_name=self.config.container_name,
                    credential=self._credential,
                    **client_kwargs,
                )

            return ContainerClient.from_connection_string(
                conn_str=self.config.connection_string,
                container_name=self.config.container_name,
                **client_kwargs,
            )
        except builtins.ValueError as err:
            LOG.error(f"Failed to create Azure Client. Error: {type(err).__name__}, Reason: {err}")
            raise err

    def _build_managed_identity_credential(self):
        """Pick the most specific managed-identity credential the config requests.

        Precedence:
          1. managed_identity_resource_id → ManagedIdentityCredential(identity_config={"resource_id": ...})
          2. managed_identity_client_id   → ManagedIdentityCredential(client_id=...)
          3. neither                      → DefaultAzureCredential() (covers system-assigned MI and local dev)
        """
        if ManagedIdentityCredential is None or DefaultAzureCredential is None:
            raise ImportError("azure-identity is required when auth_mode=managed_identity")

        if self.config.managed_identity_resource_id:
            return ManagedIdentityCredential(
                identity_config={"resource_id": self.config.managed_identity_resource_id}
            )
        if self.config.managed_identity_client_id:
            return ManagedIdentityCredential(client_id=self.config.managed_identity_client_id)
        return DefaultAzureCredential()

    def render_path(self, path: str) -> str:
        """Renders the absolute path for the Azure Blob Storage Destination

        Args:
            path (str): Relative path to the blob in the container

        Returns:
            str: Absolute path to the blob in the container
        """
        return f"{self.config.remote_path}/{path}".strip("/")

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
                self.container_client.download_blob(blob_key, max_concurrency=self.config.max_concurrency).readinto(
                    pipe_out_file
                )
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
            self.container_client.delete_blob(self.render_path(path))
        except builtins.Exception as err:
            LOG.error(f"Failed to delete blob {self.render_path(path)}. Error: {type(err).__name__}, Reason: {err}")
            raise err

    @contextmanager
    def get_stream(self, copy: BaseCopy) -> t.Generator[t.BinaryIO, None, None]:
        """Streams a blob from Azure Blob Storage into a pipe

        Args:
            copy (BaseCopy): A copy object to stream from Azure

        Yields:
            t.Generator(t.BinaryIO): A generator that yields a stream of the blob's content
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
            return self.container_client.download_blob(
                self.render_path(filepath), encoding="utf-8", max_concurrency=self.config.max_concurrency
            ).read()
        except ae.ResourceNotFoundError:
            LOG.debug(f"File {self.render_path(filepath)} does not exist in container {self.config.container_name}")
            raise FileNotFound(
                f"File {self.render_path(filepath)} does not exist in container {self.config.container_name}"
            )
        except builtins.Exception as err:
            LOG.error(f"Failed to read blob {self.render_path(filepath)}. Error: {type(err).__name__}, Reason: {err}")
            raise err

    def save(self, handler: t.BinaryIO, filepath: str) -> None:
        """Save a stream given as handler to filepath in Azure Blob Storage

        Args:
            handler (t.BinaryIO): Incoming stream
            filepath (str): Relative path to a blob in the container

        Raises:
            err: Raises an error if the blob failed to be written
        """

        LOG.debug(f"Attempting to save blob: {self.render_path(filepath)}")
        with handler as file_obj:
            try:
                self.container_client.upload_blob(
                    self.render_path(filepath), file_obj, max_concurrency=self.config.max_concurrency
                )
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
            self.container_client.upload_blob(
                self.render_path(filepath), content, overwrite=True, max_concurrency=self.config.max_concurrency
            )
        except builtins.Exception as err:
            LOG.error(f"Failed to upload or overwrite blob. Error {type(err).__name__}, Reason: {err}")
            raise err

    @contextmanager
    def cluster_lock(self, identifier: str, ttl: int = _CLUSTER_LOCK_MAX_TTL):
        """Acquire an Azure blob lease so only one cluster member runs
        the backup at a time.

        The lease blob lives at ``<identifier>/<_CLUSTER_LOCK_BLOB>``
        relative to the configured ``remote_path``. Requires the
        ``Storage Blob Data Contributor`` role on the container (i.e.
        the same permissions already required to upload backups).

        Behaviour:

        * If the lease is acquired, a background thread renews it every
          ``ttl / 2`` seconds until the context exits.
        * If another replica already holds the lease
          (``LeaseAlreadyPresent``), yields a :class:`ClusterLock` with
          ``acquired=False`` so the caller can skip the run cleanly.
        * On any other Azure error, logs a warning and yields
          ``acquired=True`` — we prefer running a (possibly duplicate)
          backup over skipping because the coordination layer flaked.

        :param identifier: Cluster-scoped identifier. Typically
            ``config.server_name``.
        :param ttl: Lease duration in seconds. Clamped to the Azure
            blob lease range ``[15, 60]``.
        """
        lease_ttl = max(_CLUSTER_LOCK_MIN_TTL, min(_CLUSTER_LOCK_MAX_TTL, int(ttl)))
        lock_key = self.render_path(f"{identifier}/{_CLUSTER_LOCK_BLOB}")
        blob_client = self.container_client.get_blob_client(lock_key)

        self._ensure_cluster_lock_blob(blob_client, lock_key)

        lease = BlobLeaseClient(blob_client)
        try:
            lease.acquire(lease_duration=lease_ttl)
        except ae.HttpResponseError as err:
            if getattr(err, "error_code", None) == "LeaseAlreadyPresent":
                LOG.info(
                    "Cluster lock %s is held by another member; skipping this run.",
                    lock_key,
                )
                yield ClusterLock(acquired=False)
                return
            LOG.warning(
                "Failed to acquire cluster lock %s (%s); proceeding without coordination.",
                lock_key,
                err,
            )
            yield ClusterLock(acquired=True)
            return
        except builtins.Exception as err:  # pragma: no cover - defensive
            LOG.warning(
                "Unexpected error while acquiring cluster lock %s (%s); proceeding without coordination.",
                lock_key,
                err,
            )
            yield ClusterLock(acquired=True)
            return

        renewer = _LeaseRenewer(lease, ttl=lease_ttl)
        renewer.start()
        try:
            yield ClusterLock(acquired=True, holder=lock_key)
        finally:
            renewer.stop()
            try:
                lease.release()
            except builtins.Exception as err:  # pragma: no cover - best-effort
                LOG.warning("Failed to release cluster lock %s: %s", lock_key, err)

    def _ensure_cluster_lock_blob(self, blob_client, lock_key: str) -> None:
        """Create the lease blob if it does not already exist.

        Azure blob leases require a blob to lease against. The blob
        itself holds no content — it exists only as a coordination
        primitive.
        """
        try:
            blob_client.upload_blob(b"", overwrite=False)
        except ae.ResourceExistsError:
            return
        except ae.HttpResponseError as err:
            # Benign races where another member created the blob first
            # can surface as a 409 with various error codes; treat any
            # "already exists" response as success.
            if getattr(err, "error_code", None) in (
                "BlobAlreadyExists",
                "LeaseIdMissing",
                "LeaseAlreadyPresent",
            ):
                return
            LOG.warning("Failed to initialise cluster lock blob %s: %s", lock_key, err)
            raise

    def _list_files(self, prefix: str = "", recursive: bool = False, files_only: bool = False) -> t.List[str]:
        """List files in the Azure Blob Storage container

        Args:
            prefix (str, optional): Filters blobs by a given prefix. Defaults to "".
            recursive (bool, optional): Not supported. Defaults to False.
            files_only (bool, optional): Excludes directories when true,
                otherwise includes files and directories. Defaults to False.
        """
        LOG.debug(
            f"""Listing files in container {self.config.container_name} with prefix={prefix.strip('/')},
              recursive={recursive}, files_only={files_only}"""
        )

        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix.strip("/"), include=["metadata"])
        except builtins.Exception as err:
            LOG.error(
                f"Failed to list files in container {self.config.container_name}. "
                f"Error: {type(err).__name__}, Reason: {err}"
            )
            raise err

        return [
            blob.name.strip(self.config.remote_path).strip("/")
            for blob in blobs
            if not files_only
            or not (bool(blob.get("metadata")) and blob.get("metadata", {}).get("hdi_isfolder") == "true")
        ]
