# -*- coding: utf-8 -*-
"""
Module for GCS destination.
"""
from contextlib import contextmanager
from functools import partial
from multiprocessing import Process
import os
from os import path as osp
import re

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.auth.exceptions import GoogleAuthError
from google.cloud.storage import Client

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import GCSDestinationError, \
    FileNotFound

GCS_CONNECT_TIMEOUT = 60
GCS_READ_TIMEOUT = 600
DEFAULT_CHUNK_SIZE = 250 * 1024 * 1024
_CHUNK_PART_REGEXP = r'/part-[0-9]{16}$'


class GCS(BaseDestination):
    """
    GCS destination class.

    :param kwargs: Keyword arguments.

    * **bucket** - (required) GCS bucket name.
    * **gc_credentials_file** - (required) GC credentials json filepath.
    * **chunk_size** - when storing a stream use this a a chunk size.
        The stream will be stored a set of chunks of this size on the GS.
    """

    # def save(self, handler, filepath):
    #     pass

    def __init__(self, **kwargs):
        self._bucket = kwargs.get('bucket')
        super(GCS, self).__init__(self.bucket)

        if 'gc_credentials_file' in kwargs:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs.get(
                'gc_credentials_file'
            )
        else:
            raise GCSDestinationError(
                'gc_credentials_file keyword argument must be defined '
                'when initializing %s class' % self.__class__.__name__
            )
        self._chunk_size = kwargs.get('chunk_size', DEFAULT_CHUNK_SIZE)
        self.__bucket_obj = None

    @property
    def bucket(self):
        """GCS bucket name."""
        return self._bucket

    def create_bucket(self):
        """Creates the bucket in gcs that will store the backups.

        :raises GCSDestinationError: if failed to create the bucket.
        :raises GCSDestinationError: If authentication error.
        """
        try:
            self._gcs_client.create_bucket(bucket_name=self.bucket)

        except (GoogleAPIError, GoogleAuthError) as err:
            raise GCSDestinationError(err)

        LOG.info('Created bucket %s', self.bucket)

    def delete(self, path):
        blobs = self._list_blob_or_chunks(path)
        if not blobs:
            raise FileNotFound('File %s does not exist.' % path)

        for blob in blobs:
            blob.delete()

    def delete_bucket(self, force=False):
        """Delete the bucket in gcs that was storing the backups.

        :param force: If the bucket is non-empty then delete the objects
            before deleting the bucket.
        :type force: bool
        :raise GCSDestinationError: if failed to delete the bucket.
        """
        try:
            self._bucket_obj.delete(force=force)

        except (GoogleAPIError, GoogleAuthError) as err:
            raise GCSDestinationError(err)

        LOG.info('Deleted bucket %s', self.bucket)

    @contextmanager
    def get_stream(self, copy):
        pipe_in, pipe_out = os.pipe()

        proc = Process(
            target=self._download_to_pipe,
            args=(copy.key, pipe_in, pipe_out)
        )
        proc.start()
        os.close(pipe_out)
        pipe_in = os.fdopen(pipe_in)
        yield pipe_in
        proc.join()

    def read(self, filepath):
        """
        Read content from a file.

        :param filepath: relative path to a file with status.
        :type filepath: str
        :return: Content of the file.
        :rtype: str
        :raises FileNotFound: if filepath doesn't exist on the destination.
        """
        obj = self._bucket_obj.blob(
            filepath
        )
        try:
            return obj.download_as_string()
        except NotFound as err:
            raise FileNotFound(err)

    def write(self, content, filepath):
        """
        Write a string passed in ``content`` to a filepath on the destination.

        :param content: String to write.
        :type content: str
        :param filepath: Relative file path on the destination.
        :type filepath: str
        """
        obj = self._bucket_obj.blob(
            filepath
        )
        obj.upload_from_string(content)

    def save(self, handler, filepath):
        """
        Read from handler and save it to GCS

        :param handler: stdout handler from backup source
        :type handler: file
        :param filepath: save backup copy in a file with this name
        :type filepath: str
        """
        with handler as f_src:
            chunk_no = 0
            for chunk in iter(partial(f_src.read, self._chunk_size), b''):
                self.write(
                    chunk,
                    osp.join(filepath, 'part-%016d' % chunk_no)
                )
                chunk_no += 1

    @property
    def _bucket_obj(self):
        if self.__bucket_obj is None:
            self.__bucket_obj = self._gcs_client.get_bucket(self.bucket)

        return self.__bucket_obj

    @_bucket_obj.setter
    def _bucket_obj(self, value):
        self.__bucket_obj = value

    @property
    def _gcs_client(self):
        """Creates an authenticated gcs client.

        :return: GCS client instance.
        :rtype: google.cloud.storage.Client
        """
        return Client()

    def _download_to_pipe(self, path, pipe_in, pipe_out):
        os.close(pipe_in)
        pipe_out = os.fdopen(pipe_out, 'w')

        for blob in self._list_blob_or_chunks(path):
            blob.download_to_file(pipe_out)

    def _list_blob_or_chunks(self, path):
        """
        Method queries GS and returns a list of GS blobs that match path.
        If a file is not chunked the method will return one blob in a list.
        If the file is chunked the method will return a list of blobs, each
        of them is a chunk blob.

        For example, if the path is 'master1/status'. This file is not chunked,
        so the method will return ``[Blob('master1/status')]``.
        However, if the path is
        'master1/daily/mysql/mysql-2019-04-04_05_29_05.xbstream.gz'
        the method will return each chunk as a blob. So, the return value
        will be something like::

        [
            Blob(
            master1/daily/mysql/mysql-2019-04-04_05_29_05.xbstream.gz/part..0
            ),
            Blob(
            master1/daily/mysql/mysql-2019-04-04_05_29_05.xbstream.gz/part..1
            )
            ,
            ...
        ]

        :param path: path to a file in GS
        :return: list of blobs that store this file, either the file itself or
            its chunks.
        :rtype: list(Blob)
        """
        result = []
        blobs = self._bucket_obj.list_blobs(prefix=path)
        for blob in blobs:
            if blob.name == path \
                    or re.match(path + _CHUNK_PART_REGEXP, blob.name):
                result.append(blob)
        return result

    def _list_files(self, prefix=None, recursive=False, files_only=False):
        """
        Get list of objects on Google Storage. It will remove the "/part-"
        part from the names returning one file if it was split in chunks.

        :param prefix: A prefix inside te GS bucket. For example,
            if full object path is ``gs://some-bucket/foo/bar/file.txt``
            then the path can be ``foo/`` or ``foo/bar``. Either path will
            return ``gs://some-bucket/foo/bar/file.txt``.
        :param recursive: Not used.
        :param files_only: Not used.
        :return: list of object names prefixed with ``gs://``.
        :rtype: set(str)
        """
        if prefix:
            prefix = prefix.lstrip('gs://').lstrip(self.bucket).lstrip('/')

        return set(
            [
                osp.join(
                    "gs://",
                    self.bucket,
                    re.sub(_CHUNK_PART_REGEXP, '', blob.name)
                )
                for blob in self._bucket_obj.list_blobs(
                    prefix=prefix or None
                )
            ]
        )
