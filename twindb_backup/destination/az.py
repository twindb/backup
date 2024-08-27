# -*- coding: utf-8 -*-
"""
Module for Azure destination.
"""
import builtins
import os
import re
import socket
import time
from contextlib import contextmanager
from multiprocessing import Process
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import FileNotFound, S3DestinationError
from twindb_backup.exceptions import OperationError

"""
DEFAULT VALUES SECTION
"""


class AZFileAccess(object):  # pylint: disable=too-few-public-methods
    """Access modes for AZ files"""

    public_read = "public-read"
    private = "private"


class AZ(BaseDestination):
    """
    AZ destination class.

    :param kwargs: Keyword arguments.

    * **container_name** - Azure container name
    * **connection_string** - Azure connection string for the storage account
    * **hostname** - Hostname of a host where a backup is taken from.
    * **chunk_size** - The size in byptes for read/write streams, default 4MB
    """

    def __init__(self, **kwargs):

        self._container_name = kwargs.get("container_name")
        self._connection_string = kwargs.get("connection_string")
        self._hostname = kwargs.get("hostname", socket.gethostname())
        self._chunk_size = kwargs.get("chunk_size", 4 * 1024 * 1024)

        self.remote_path = "/"
        super(AZ, self).__init__(self.remote_path)

        try:
            LOG.debug(
                "Initilizing Azure connection to the storage account using connection string (length="
                + str(len(self._connection_string))
                + ")"
            )
            self.service_client = BlobServiceClient.from_connection_string(self._connection_string)
        except builtins.Exception as err:
            # TODO: add more specific exception handling
            LOG.error("Failed to connect to Azure storage account using the connection string")
            raise err

        # Check to see if the container exists, otherwise create the container
        try:
            LOG.debug("Setting up the container(" + self._container_name + ") connection")
            self.client = self.service_client.get_container_client(self._container_name)
        except builtins.Exception:
            LOG.debug("The container(" + self._container_name + ") does not exist... creating it")
            self.create_container()

    def bucket(self):
        """S3 bucket name.... compatibility???"""
        return self._container_name

    def create_bucket(self):
        """Compatibility."""
        return create_container(self)

    def create_container(self):
        """Creates the container in the Azure storage account that will store the backups."""
        container_exists = True

        try:
            self.client = self.service_client.create_container(self._container_name)
        except ClientError as err:
            # We come here meaning we did not find the container
            raise

        LOG.info("Azure container creation was successful %s", self._container_name)
        return True

    def list_files(self, prefix=None, recursive=False, pattern=None, files_only=False):
        """
        List files in the destination that have common prefix.
        """

        files = []
        LOG.debug("AZ Listing files")
        for blob in self.client.list_blobs():
            if pattern:
                if re.search(pattern, blob.name):
                    files.append(blob.name)
            else:
                files.append(blob.name)

        return sorted(files)

    def _list_files(self, prefix=None, recursive=False, files_only=False):
        raise NotImplementedError

    def _uplaod_blob_options(self, **kwargs):
        return True

    def delete(self, path):
        """
        Deletes a Azure blob in the container
        """

        try:
            LOG.info("Deleting blob: " + path)
            return self.client.delete_blob(path)
        except builtins.Exception as err:
            # TODO: add more specific exception handling
            LOG.error("FAILED to delete blob: " + path)
            raise err

    def delete_all_objects(self):
        """
        Delete all blobs in the container
        """
        LOG.info("Deleting ALL blobs in container: " + self._container_name)
        for blob in self.ls():
            self.delete(blob)

        return True

    def delete_bucket(self, force=False):
        """
        Delete the container and contents, this is a recursive delete (including all blobs in the container)
        """
        try:
            LOG.info("Performing recusrsive delete of container and all blobs in container: " + self._container_name)
            self.client.delete_container()
        except builtins.Exception as err:
            # TODO: add more specific exception handling
            raise err

        return True

    def read(self, filepath):
        """
        Read the status blob (filepath)  and return contents to the caller
        """
        try:
            LOG.debug("Attempting to read blob: " + filepath)
            blob_client = self.client.get_blob_client(filepath)
            return blob_client.download_blob().readall().decode("utf-8")

        except builtins.Exception as err:
            # TODO: add more specific exception handling
            LOG.info("The blob " + filepath + " does not exist or there was an issue reading it")
            raise err

    def save(self, handler, filepath):
        """
        Read from handler and save it to Azure blob

        :param filepath: save backup copy in a file with this name
        :param handler: stdout handler from backup source
        """

        LOG.debug("Attempting to write blob: " + filepath)
        blob_client = self.client.get_blob_client(filepath)

        with handler as file_obj:
            try:
                blob_client.upload_blob(file_obj)

            except builtins.Exception as err:
                # TODO: add more specific exception handling
                LOG.info("The blob " + filepath + " already exists, no need to upload (ignoring)")
                raise err

    @contextmanager
    def get_stream(self, copy):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination.
        :param copy: Backup copy
        :type copy: BaseCopy
        :return: Stream with backup copy
        :rtype: generator
        :raise : if failed to stream a backup copy.
        """

        path = "%s/%s" % (self.remote_path, copy.key)
        object_key = urlparse(path).path.lstrip("/")

        def _download_object(self, path, read_fd, write_fd):
            # The read end of the pipe must be closed in the child process
            # before we start writing to it.
            os.close(read_fd)

            # twindb appears to be munging the actual path of the objects as opposed to
            # using the list of the valid object paths ... wtf?
            # anyway... let's decompile it, grab the host and the actual file name
            # then do some matching based on what really exists :P
            LOG.debug("Transforming requested restore path: " + path)
            exploded_path = path.split("/")
            host = exploded_path[1]  # first element, the call path begins with /
            file = exploded_path[len(exploded_path) - 1]  # last element
            path = ""
            for blob in self.list_files(pattern=file):
                if re.search(host, blob):
                    path = blob

            LOG.debug("Tranformed path to match existing blob: " + path)

            blob_client = self.client.get_blob_client(path)
            with os.fdopen(write_fd, "wb") as w_pipe:
                try:
                    retry_interval = 2
                    for _ in range(10):
                        try:
                            w_pipe.write(blob_client.download_blob().readall())
                        except builtins.Exception as err:
                            # TODO: add more specific exception handling
                            LOG.error(f"Failed to download and write blob {path} encountered error: {err}")
                            raise err

                except IOError as err:
                    LOG.error(err)
                    raise err

                except builtins.Exception as err:
                    # TODO: add more specific exception handling
                    raise err

        download_proc = None
        try:
            blob_client = self.client.get_blob_client(path)
            LOG.debug("Fetching blob %s from container %s", path, self._container_name)

            read_pipe, write_pipe = os.pipe()

            download_proc = Process(
                target=_download_object,
                args=(
                    self,
                    path,
                    read_pipe,
                    write_pipe,
                ),
                name="_download_object",
            )
            download_proc.start()

            # The write end of the pipe must be closed in this process before
            # we start reading from it.
            os.close(write_pipe)
            LOG.debug("read_pipe type: %s", type(read_pipe))
            yield read_pipe

            os.close(read_pipe)
            download_proc.join()

            if download_proc.exitcode:
                LOG.error("Failed to download %s", path)
                # exit(1)

            LOG.debug("Successfully streamed %s", path)

        finally:
            if download_proc:
                download_proc.join()

    def write(self, content, filepath):
        LOG.debug("Overwriting status file: " + filepath)
        blob_client = self.client.get_blob_client(filepath)
        blob_client.upload_blob(content, overwrite=True)
