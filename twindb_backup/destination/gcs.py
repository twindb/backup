# -*- coding: utf-8 -*-
"""
Module for GCS destination.
"""
import os
import re
import socket

from contextlib import contextmanager
from multiprocessing import Process
from urlparse import urlparse

import time

from google.cloud import storage, exceptions

from twindb_backup import LOG, TwinDBBackupError
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import GCSDestinationError
from twindb_backup.status.mysql_status import MySQLStatus

GCS_CONNECT_TIMEOUT = 60
GCS_READ_TIMEOUT = 600


class GCSFileAccess(object):  # pylint: disable=too-few-public-methods
    """Access modes for GCS files"""
    public_read = 'public-read'
    private = 'private'


class GCS(BaseDestination):
    """
    GCS destination class.

    :param kwargs: Keyword arguments.

    * **bucket** - GCS bucket name.
    * **gc_credentials_file** - GC credentials json filepath.
    * **gc_encryption_key** - GC encryption key if used.
    * **hostname** - Hostname of a host where a backup is taken from.
    """
    def __init__(self, **kwargs):
        self._bucket = kwargs.get('bucket')
        self._hostname = kwargs.get('hostname', socket.gethostname())

        self.remote_path = 'gs://{bucket}'.format(
            bucket=self._bucket
        )
        super(GCS, self).__init__(self.remote_path)

        credentials_file = kwargs.get('gc_credentials_file')
        if credentials_file is not None:
            os.environ["GC_CREDENTIALS_FILE"] = credentials_file

        encryption_key = kwargs.get('gc_encryption_key')
        if encryption_key is not None:
            os.environ["GC_ENCRYPTION_KEY"] = encryption_key

        # Setup an authenticated GCS client that we will use throughout
        self.gcs_client = self.setup_gcs_client()

    @property
    def bucket(self):
        """GCS bucket name."""
        return self.bucket

    @property
    def encryption_key(self):
        """GCS encryption key"""
        """
        If no key is given, it must be set to None
        """
        return None if "GC_ENCRYPTION_KEY" not in os.environ or os.environ["GC_ENCRYPTION_KEY"] == "" \
            else os.environ["GC_ENCRYPTION_KEY"]

    def status_path(self, cls=MySQLStatus):
        """
        Return key path where status is stored for a given type of status.

        :param cls: status class. By default MySQLStatus
        :return: key name in GCS bucket where status is stored.
        :rtype: str
        """
        return "{hostname}/{basename}".format(
            hostname=self._hostname,
            basename=cls().basename
        )

    def setup_gcs_client(self):
        """Creates an authenticated gcs client.

        :return: GCS client instance.
        :rtype: google.cloud.storage.Client
        """
        if "GC_CREDENTIALS_FILE" in os.environ and os.environ["GC_CREDENTIALS_FILE"] is not None:
            client = storage.Client.from_service_account_json(os.environ["GC_CREDENTIALS_FILE"])
        else:
            client = storage.Client.create_anonymous_client()

        return client

    def get_bucket(self):
        """Get bucket object by name

        :return: GCS bucket instance.
        :rtype: google.cloud.storage.Bucket
        """
        return self.gcs_client.get_bucket(bucket_name=self.bucket)

    def create_bucket(self):
        """Creates the bucket in gcs that will store the backups.

        :raise GCSDestinationError: if failed to create the bucket.
        """

        try:
            self.gcs_client.create_bucket(bucket_name=self.bucket)
            LOG.info('Created bucket %s', self.bucket)
        except exceptions.Conflict:
            # If bucket exists, move on
            LOG.info('Bucket %s already exists', self.bucket)

        LOG.info('Created bucket %s', self.bucket)

        return True

    def delete_bucket(self, force=False):
        """Delete the bucket in gcs that was storing the backups.

        :param force: If the bucket is non-empty then delete the objects
            before deleting the bucket.
        :type force: bool
        :raise GCSDestinationError: if failed to delete the bucket.
        """
        bucket_exists = True
        bucket = None

        try:
            bucket = self.get_bucket()
        except exceptions.NotFound:
            bucket_exists = False

        if bucket_exists:
            LOG.info('Deleting bucket %s', self.bucket)

            if force:
                LOG.info('Deleting the objects in the bucket %s', self.bucket)
                self.delete_all_objects()

            try:
                bucket.delete(force=force)
            except exceptions.GoogleCloudError as err:
                self.validate_client_error(err)

            LOG.info('Bucket %s successfully deleted', self.bucket)

        return True

    def delete_all_objects(self):
        """
        Delete all objects from GCS bucket.

        :raise GCSDestinationError: if failed to delete objects from the bucket.
        """
        bucket = self.get_bucket()
        bucket.delete_blobs(list(bucket.list_blobs()))

        return True

    def save(self, handler, name):
        """
        Read from handler and save it to GCS

        :param name: save backup copy in a file with this name
        :param handler: stdout handler from backup source
        """
        with handler as file_obj:
            ret = self._upload_object(file_obj, name)
            LOG.debug('Returning code %d', ret)

    def list_files(self, prefix, recursive=False, pattern=None, files_only=False):
        """
        List files in the destination that have common prefix.

        :param prefix: Common prefix. May include the bucket name.
            (e.g. ``gs://my_bucket/foo/``) or simply a prefix in the bucket
            (e.g. ``foo/``).
        :type prefix: str
        :param recursive: Does nothing for this class.
        :return: sorted list of file names.
        :param pattern: files must match with this regexp if specified.
        :type pattern: str
        :param files_only: Does nothing for this class.
        :return: Full GCS url in form ``gs://bucket/path/to/file``.
        :rtype: list(str)
        :raise GCSDestinationError: if failed to list files.
        """
        bucket = self.get_bucket()

        LOG.debug('Listing bucket %s', self.bucket)
        LOG.debug('prefix = %s', prefix)

        norm_prefix = prefix.replace('gs://%s' % bucket.name, '')
        norm_prefix = norm_prefix.lstrip('/')
        LOG.debug('normal prefix = %s', norm_prefix)

        # Try to list the bucket several times
        # because of intermittent error NoSuchBucket:
        # https://travis-ci.org/twindb/backup/jobs/204053690
        expire = time.time() + GCS_READ_TIMEOUT
        retry_interval = 2
        while time.time() < expire:
            try:
                files = []
                all_objects = bucket.list_blobs(prefix=norm_prefix)

                for file_object in all_objects:
                    if pattern:
                        if re.search(pattern, file_object.name):
                            files.append(
                                'gs://{bucket}/{key}'.format(
                                    bucket=self.bucket,
                                    key=file_object.name
                                )
                            )
                    else:
                        files.append(
                            'gs://{bucket}/{key}'.format(
                                bucket=self.bucket,
                                key=file_object.name
                            )
                        )

                return sorted(files)
            except exceptions.GoogleCloudError as err:
                LOG.warning(
                    '%s. Will retry in %d seconds.',
                    err,
                    retry_interval
                )
                time.sleep(retry_interval)
                retry_interval *= 2

        raise GCSDestinationError('Failed to list files.')

    def _list_files(self, path, recursive=False, files_only=False):
        raise NotImplementedError

    def delete(self, obj):
        """Deletes an GCS object.

        :param obj: Key of GCS object.
        :type obj: str
        :raise GCSDestinationError: if failed to delete object.
        """
        key = obj.replace(
            'gs://%s/' % self.bucket,
            ''
        ) if obj.startswith('gs://') else obj

        bucket = self.get_bucket()
        gcsobj = bucket.blob(obj, encryption_key=self.encryption_key)

        LOG.debug('deleting gs://%s/%s', bucket.name, key)
        gcsobj.delete()

        return True

    @contextmanager
    def get_stream(self, copy):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination.

        :param copy: Backup copy
        :type copy: storage.copy.BaseCopy
        :return: Stream with backup copy
        :rtype: generator
        :raise GCSDestinationError: if failed to stream a backup copy.
        """

        path = "%s/%s" % (self.remote_path, copy.key)
        object_key = urlparse(path).path.lstrip('/')

        def _download_object(gcs_client, bucket_name, key, encryption_key, read_fd, write_fd):
            """download object.

            :param gcs_client:
            :type gcs_client: google.cloud.storage.Client
            :param bucket_name:
            :param key:
            :param read_fd:
            :param write_fd:
            :return:
            """

            # The read end of the pipe must be closed in the child process
            # before we start writing to it.
            os.close(read_fd)

            with os.fdopen(write_fd, 'wb') as w_pipe:
                try:
                    retry_interval = 2
                    for _ in xrange(10):
                        try:
                            obj = gcs_client.get_bucket(bucket_name).get_blob(key, encryption_key=encryption_key)
                            obj.download_to_file(w_pipe)

                            return
                        except exceptions.GoogleCloudError as err:
                            LOG.warning(err)
                            LOG.warning('Will retry in %d seconds',
                                        retry_interval)
                            time.sleep(retry_interval)
                            retry_interval *= 2
                except IOError as err:
                    LOG.error(err)
                    exit(1)

        download_proc = None

        try:
            LOG.debug('Fetching object %s from bucket %s',
                      object_key,
                      self.bucket)

            read_pipe, write_pipe = os.pipe()

            download_proc = Process(target=_download_object,
                                    args=(self.gcs_client, self.bucket,
                                          self.encryption_key, object_key, read_pipe, write_pipe),
                                    name='_download_object')
            download_proc.start()

            # The write end of the pipe must be closed in this process before
            # we start reading from it.
            os.close(write_pipe)
            LOG.debug('read_pipe type: %s', type(read_pipe))
            yield read_pipe

            os.close(read_pipe)
            download_proc.join()

            if download_proc.exitcode:
                LOG.error('Failed to download %s', path)
                # exit(1)

            LOG.debug('Successfully streamed %s', path)

        finally:
            if download_proc:
                download_proc.join()

    def _upload_object(self, file_obj, object_key):
        """Upload objects to GCS in streaming fashion.

        :param file file_obj: A file like object to upload. At a minimum, it
            must implement the read method, and must return bytes.
        :param str object_key: The destination key where to upload the object.
        :raise GCSDestinationError: if failed to upload object.
        """
        remote_name = "gs://{bucket}/{name}".format(
            bucket=self.bucket,
            name=object_key
        )

        LOG.debug("Starting to stream to %s", remote_name)
        try:
            blob = self.get_bucket().blob(object_key, encryption_key=self.encryption_key)
            blob.upload_from_file(file_obj)

            LOG.debug("Successfully streamed to %s", remote_name)
        except exceptions.GoogleCloudError as err:
            raise GCSDestinationError(err)

        return self._validate_upload(object_key)

    def _validate_upload(self, object_key):
        """
        Validates that upload of an object was successful.
        Raises an exception if the response code is not 200.

        :raise GCSDestinationError: if object is not available on
            the destination.
        """
        remote_name = "gs://{bucket}/{name}".format(
            bucket=self.bucket,
            name=object_key
        )

        LOG.debug("Validating upload to %s", remote_name)

        if not self.get_bucket().blob(object_key, encryption_key=self.encryption_key).exists():
            raise GCSDestinationError(exceptions.NotFound("Object %s not found" % remote_name))

        LOG.debug("Upload successfully validated")

        return 0

    def _status_exists(self, cls=MySQLStatus):
        try:
            status_object = self.get_bucket().get_blob(self.status_path(cls=cls), encryption_key=self.encryption_key)

            if status_object is None:
                return False

            if status_object.size > 0:
                return True
        except exceptions.NotFound:
            return False

    def _read_status(self, cls=MySQLStatus):
        if self._status_exists(cls=cls):
            try:
                obj = self.get_bucket().blob(self.status_path(), encryption_key=self.encryption_key)
            except exceptions.GoogleCloudError as err:
                raise GCSDestinationError(err)

            content = obj.download_as_string()
            return cls(content=content)
        else:
            return cls()

    def _write_status(self, status, cls=MySQLStatus):
        obj = self.get_bucket().blob(self.status_path(cls=cls), encryption_key=self.encryption_key)
        obj.upload_from_string(status.serialize())

    @staticmethod
    def validate_client_error(error):
        """Encapsulates error response from gcs

        :param error: The response that needs to be validated.
        :type error: google.cloud.exceptions.GoogleCloudError
        :raise GCSDestinationError: if response from GCS is invalid.
        """
        raise GCSDestinationError('GCS client returned error code: %s description: %s',
                                  error.grpc_status_code,
                                  error.message)

    def _set_file_access(self, access_mode, url):
        """
        Set file access via GCS url

        :param access_mode: Access mode
        :type access_mode: str
        :param url: GCS url
        :type url: str
        """
        object_key = urlparse(url).path.lstrip('/')
        obj = self.get_bucket().blob(object_key, encryption_key=self.encryption_key)
        if access_mode == GCSFileAccess.public_read:
            obj.make_public()
        else:
            obj.make_private()

    def _get_file_url(self, gcs_url):
        """
        Generate public url via GCS url
        :param gcs_url: GCS url
        :type gcs_url: str
        :return: Public url
        :rtype: str
        """
        object_key = urlparse(gcs_url).path.lstrip('/')
        obj = self.get_bucket().blob(object_key, encryption_key=self.encryption_key)
        return obj.public_url

    def share(self, gcs_url):
        """
        Share GCS file and return public link

        :param gcs_url: GCS url
        :type gcs_url: str
        :return: Public url
        :rtype: str
        :raise GCSDestinationError: if failed to share object.
        """
        LOG.debug('Checking blob uri %s', gcs_url)
        norm_prefix = gcs_url.replace('gs://%s' % self.bucket, '')
        norm_prefix = norm_prefix.lstrip('/')
        LOG.debug('normal prefix = %s', norm_prefix)

        obj = self.get_bucket().get_blob(norm_prefix, encryption_key=self.encryption_key)
        if obj is None:
            raise TwinDBBackupError("File not found via url: %s" % gcs_url)
        else:
            self._set_file_access(GCSFileAccess.public_read, gcs_url)
            return self._get_file_url(gcs_url)

    def _get_file_content(self, path):
        attempts = 10  # up to 1024 seconds
        sleep_time = 2
        while sleep_time <= 2 ** attempts:
            try:
                obj = self.get_bucket().get_blob(path, encryption_key=self.encryption_key)
                content = obj.download_as_string()
                return content
            except exceptions.GoogleCloudError as err:
                LOG.warning('Failed to read gs://%s/%s', self.bucket, path)
                LOG.warning(err)
                LOG.info('Will try again in %d seconds', sleep_time)
                time.sleep(sleep_time)
                sleep_time *= 2

        msg = 'Failed to read gs://%s/%s after %d attempts' \
              % (self.bucket, path, attempts)
        raise TwinDBBackupError(msg)

    def _move_file(self, source, destination):
        obj = self.get_bucket().blob(source, encryption_key=self.encryption_key)
        self.get_bucket().rename_blob(obj, destination)
