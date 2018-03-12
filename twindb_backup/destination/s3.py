# -*- coding: utf-8 -*-
"""
Module for S3 destination.
"""
import os
import socket

from contextlib import contextmanager
from multiprocessing import Process
from urlparse import urlparse

import time

from botocore.exceptions import ClientError
from botocore.client import Config

import boto3
from boto3.s3.transfer import TransferConfig

from twindb_backup import LOG, TwinDBBackupError
from twindb_backup.destination.base_destination import BaseDestination
from twindb_backup.destination.exceptions import S3DestinationError, \
    StatusFileError

S3_CONNECT_TIMEOUT = 60
S3_READ_TIMEOUT = 600

# The transfer size threshold for which multipart uploads to S3/Ceph RGW will
# automatically be triggered
S3_UPLOAD_CHUNK_SIZE_BYTES = 256 * 1024 ** 2

# The maximum number of threads that will be making requests to perform a
# transfer.
S3_UPLOAD_CONCURRENCY = 4

# The maximum amount of read parts that can be queued in memory to be written
# for a download. The size of each of these read parts is at most the size of
# ``S3_UPLOAD_IO_CHUNKS_SIZE_BYTES``.
S3_UPLOAD_IO_QUEUE_SIZE = 200

# The max size of each chunk in the io queue.
S3_UPLOAD_IO_CHUNKS_SIZE_BYTES = 256 * 1024


class AWSAuthOptions(object):  # pylint: disable=too-few-public-methods
    """Class to store AWS credentials"""
    def __init__(self, access_key_id, secret_access_key,
                 default_region='us-east-1'):
        self.default_region = default_region
        self.secret_access_key = secret_access_key
        self.access_key_id = access_key_id


class S3FileAccess(object):  # pylint: disable=too-few-public-methods
    """Access modes for S3 files"""
    public_read = 'public-read'
    private = 'private'


class S3(BaseDestination):
    """
    S3 destination class.

    :param bucket: Bucket name.
    :type bucket: str
    :param aws_options: AWS credentials.
    :type aws_options: AWSAuthOptions
    :param hostname: Hostname of a host where a backup is taken from.
    """
    def __init__(self, bucket, aws_options, hostname=socket.gethostname()):

        super(S3, self).__init__()
        self.bucket = bucket
        self.remote_path = 's3://{bucket}'.format(bucket=self.bucket)

        self.aws = aws_options

        os.environ["AWS_ACCESS_KEY_ID"] = self.aws.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.aws.secret_access_key
        os.environ["AWS_DEFAULT_REGION"] = self.aws.default_region

        self.status_path = "{hostname}/status".format(
            hostname=hostname
        )
        self.status_tmp_path = self.status_path + ".tmp"

        # Setup an authenticated S3 client that we will use throughout
        self.s3_client = self.setup_s3_client()

    def setup_s3_client(self):
        """Creates an authenticated s3 client.

        :return: S3 client instance.
        :rtype: botocore.client.BaseClient
        """
        session = boto3.Session(
            aws_access_key_id=self.aws.access_key_id,
            aws_secret_access_key=self.aws.secret_access_key
        )
        s3_config = Config(
            connect_timeout=S3_CONNECT_TIMEOUT,
            read_timeout=S3_READ_TIMEOUT
        )
        client = session.client('s3', region_name=self.aws.default_region,
                                config=s3_config)

        return client

    def create_bucket(self):
        """Creates the bucket in s3 that will store the backups.

        :raise S3DestinationError: if failed to create the bucket.
        """
        bucket_exists = True

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError as err:
            # We come here meaning we did not find the bucket
            if err.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                bucket_exists = False
            else:
                raise

        if not bucket_exists:
            LOG.info('Created bucket %s', self.bucket)
            response = self.s3_client.create_bucket(Bucket=self.bucket)
            self.validate_client_response(response)

        return True

    def delete_bucket(self, force=False):
        """Delete the bucket in s3 that was storing the backups.

        :param force: If the bucket is non-empty then delete the objects
            before deleting the bucket.
        :type force: bool
        :raise S3DestinationError: if failed to delete the bucket.
        """
        bucket_exists = True

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError as err:
            # We come here meaning we did not find the bucket
            if err.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                bucket_exists = False
            else:
                raise

        if bucket_exists:
            LOG.info('Deleting bucket %s', self.bucket)

            if force:
                LOG.info('Deleting the objects in the bucket %s', self.bucket)
                self.delete_all_objects()

            response = self.s3_client.delete_bucket(Bucket=self.bucket)
            self.validate_client_response(response)

            LOG.info('Bucket %s successfully deleted', self.bucket)

        return True

    def delete_all_objects(self):
        """
        Delete all objects from S3 bucket.

        :raise S3DestinationError: if failed to delete objects from the bucket.
        """
        paginator = self.s3_client.get_paginator('list_objects')
        response = paginator.paginate(Bucket=self.bucket)

        for item in response.search('Contents'):
            if not item:
                continue

            self.s3_client.delete_object(Bucket=self.bucket, Key=item['Key'])

        return True

    def save(self, handler, name):
        """
        Read from handler and save it to Amazon S3

        :param name: save backup copy in a file with this name
        :param handler: stdout handler from backup source
        :raise S3DestinationError: if failed to save the backup.
        """
        try:
            with handler as file_obj:
                ret = self._upload_object(file_obj, name)
                LOG.debug('Returning code %d', ret)
                return ret == 0
        except S3DestinationError as err:
            LOG.error('S3 upload failed: %s', err)
            raise err

    def list_files(self, prefix, recursive=False):
        """
        List files in the destination that have common prefix.

        :param prefix: Common prefix.
        :type prefix: str
        :param recursive: Does nothing for this class.
        :return: sorted list of file names
        :rtype: list(str)
        :raise S3DestinationError: if failed to list files.
        """
        s3client = boto3.resource('s3')
        bucket = s3client.Bucket(self.bucket)

        LOG.debug('Listing %s in bucket %s', prefix, self.bucket)

        norm_prefix = prefix.replace('s3://%s/' % bucket.name, '')
        LOG.debug('norm_prefix = %s', norm_prefix)

        # Try to list the bucket several times
        # because of intermittent error NoSuchBucket:
        # https://travis-ci.org/twindb/backup/jobs/204053690
        retry_timeout = time.time() + S3_READ_TIMEOUT
        retry_interval = 2
        while time.time() < retry_timeout:
            try:
                files = []
                all_objects = bucket.objects.filter(Prefix=norm_prefix)
                for file_object in all_objects:
                    files.append(file_object.key)
                return sorted(files)
            except ClientError as err:
                LOG.warning('%s. Will retry in %d seconds.',
                            err, retry_interval)
                time.sleep(retry_interval)
                retry_interval *= 2

        raise S3DestinationError('Failed to list files.')

    def find_files(self, prefix, run_type):
        """
        Find files with common prefix and given run type.

        :param prefix: Common prefix.
        :type prefix: str
        :param run_type: daily, hourly, etc
        :type run_type: str
        :return: list of file names
        :rtype: list(str)
        :raise S3DestinationError: if failed to find files.
        """
        s3client = boto3.resource('s3')
        bucket = s3client.Bucket(self.bucket)
        LOG.debug('Listing %s in bucket %s', prefix, bucket)

        # Try to list the bucket several times
        # because of intermittent error NoSuchBucket:
        # https://travis-ci.org/twindb/backup/jobs/204066704
        retry_timeout = time.time() + S3_READ_TIMEOUT
        retry_interval = 2
        while time.time() < retry_timeout:
            try:
                files = []
                all_objects = bucket.objects.filter(Prefix='')
                for file_object in all_objects:
                    if "/" + run_type + "/" in file_object.key:
                        files.append("s3://%s/%s" % (self.bucket,
                                                     file_object.key))

                return sorted(files)
            except ClientError as err:
                LOG.warning('%s. Will retry in %d seconds.',
                            err, retry_interval)
                time.sleep(retry_interval)
                retry_interval *= 2

            except Exception as err:
                LOG.error('Failed to list objects in bucket %s: %s',
                          self.bucket, err)
                raise

        raise S3DestinationError('Failed to find files.')

    def delete(self, obj):
        """Deletes an S3 object.

        :param obj: Key of S3 object.
        :type obj: str
        :raise S3DestinationError: if failed to delete object.
        """
        s3client = boto3.resource('s3')
        bucket = s3client.Bucket(self.bucket)

        s3obj = s3client.Object(bucket.name, obj)
        LOG.debug('deleting s3://%s/%s', bucket.name, obj)

        return s3obj.delete()

    @contextmanager
    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination.

        :return: Stream with backup copy
        :rtype: generator
        :raise S3DestinationError: if failed to stream a backup copy.
        """
        object_key = urlparse(path).path.lstrip('/')

        def _download_object(s3_client, bucket_name, key, read_fd, write_fd):
            # The read end of the pipe must be closed in the child process
            # before we start writing to it.
            os.close(read_fd)

            with os.fdopen(write_fd, 'wb') as w_pipe:
                try:
                    retry_interval = 2
                    for _ in xrange(10):
                        try:
                            s3_client.download_fileobj(bucket_name,
                                                       key,
                                                       w_pipe)
                            return
                        except ClientError as err:
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
                                    args=(self.s3_client, self.bucket,
                                          object_key, read_pipe, write_pipe),
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
        """Upload objects to S3 in streaming fashion.

        :param file file_obj: A file like object to upload. At a minimum, it
            must implement the read method, and must return bytes.
        :param str object_key: The destination key where to upload the object.
        :raise S3DestinationError: if failed to upload object.
        """
        remote_name = "s3://{bucket}/{name}".format(
            bucket=self.bucket,
            name=object_key
        )

        LOG.debug("Generating S3 transfer config")
        s3_transfer_config = self.get_transfer_config()

        LOG.debug("Starting to stream to %s", remote_name)
        try:
            self.s3_client.upload_fileobj(file_obj,
                                          self.bucket,
                                          object_key,
                                          Config=s3_transfer_config)
            LOG.debug("Successfully streamed to %s", remote_name)
        except ClientError as err:
            raise S3DestinationError(err)

        return self._validate_upload(object_key)

    def _validate_upload(self, object_key):
        """
        Validates that upload of an object was successful.
        Raises an exception if the response code is not 200.

        :raise S3DestinationError: if object is not available on
            the destination.
        """
        remote_name = "s3://{bucket}/{name}".format(
            bucket=self.bucket,
            name=object_key
        )

        LOG.debug("Validating upload to %s", remote_name)

        response = self.s3_client.get_object(Bucket=self.bucket,
                                             Key=object_key)
        self.validate_client_response(response)

        LOG.debug("Upload successfully validated")

        return 0

    def _status_exists(self):
        s3client = boto3.resource('s3')
        status_object = s3client.Object(self.bucket, self.status_path)
        try:
            if status_object.content_length > 0:
                return True
        except ClientError as err:
            if err.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                return False
            else:
                raise
        return False

    def _write_status(self, status):
        for i in range(0, 3):
            response = self.s3_client.put_object(Body=status,
                                                 Bucket=self.bucket,
                                                 Key=self.status_tmp_path)
            self.validate_client_response(response)
            if self._move_or_wait(3 * i):
                return
        raise StatusFileError("Valid status file not found")

    @staticmethod
    def validate_client_response(response):
        """Validates the response returned by the client. Raises an exception
            if the response code is not 200 or 204

        :param response: The response that needs to be validated.
        :type response: dict
        :raise S3DestinationError: if response from S3 is invalid.
        """
        try:
            http_status_code = response['ResponseMetadata']['HTTPStatusCode']
        except KeyError as err:
            raise S3DestinationError('S3 client returned invalid response: %s'
                                     % err)

        if http_status_code not in [200, 204]:
            raise S3DestinationError('S3 client returned error code: %s'
                                     % http_status_code)

    @staticmethod
    def get_transfer_config():
        """
        Build Transfer config

        :return: Transfer config
        :rtype: boto3.s3.transfer.TransferConfig
        """
        transfer_config = TransferConfig(
            multipart_threshold=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_concurrency=S3_UPLOAD_CONCURRENCY,
            multipart_chunksize=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_io_queue=S3_UPLOAD_IO_QUEUE_SIZE,
            io_chunksize=S3_UPLOAD_IO_CHUNKS_SIZE_BYTES)

        return transfer_config

    def _set_file_access(self, access_mode, url):
        """
        Set file access via S3 url

        :param access_mode: Access mode
        :type access_mode: str
        :param url: S3 url
        :type url: str
        """
        object_key = urlparse(url).path.lstrip('/')
        self.s3_client.put_object_acl(Bucket=self.bucket, ACL=access_mode,
                                      Key=object_key)

    def _get_file_url(self, s3_url):
        """
        Generate public url via S3 url
        :param s3_url: S3 url
        :type s3_url: str
        :return: Public url
        :rtype: str
        """
        object_key = urlparse(s3_url).path.lstrip('/')
        return self.s3_client.generate_presigned_url('get_object',
                                                     Params={
                                                         'Bucket': self.bucket,
                                                         'Key': object_key
                                                     })

    def share(self, s3_url):
        """
        Share S3 file and return public link

        :param s3_url: S3 url
        :type s3_url: str
        :return: Public url
        :rtype: str
        :raise S3DestinationError: if failed to share object.
        """
        run_type = s3_url.split('/')[4]
        backup_urls = self.find_files(self.remote_path, run_type)
        if s3_url in backup_urls:
            self._set_file_access(S3FileAccess.public_read, s3_url)
            return self._get_file_url(s3_url)
        else:
            raise TwinDBBackupError("File not found via url: %s", s3_url)

    def _get_file_content(self, path):
        attempts = 10  # up to 1024 seconds
        sleep_time = 2
        while sleep_time <= 2**attempts:
            try:
                response = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=path
                )
                self.validate_client_response(response)

                content = response['Body'].read()
                return content
            except ClientError as err:
                LOG.warning('Failed to read s3://%s/%s', self.bucket, path)
                LOG.warning(err)
                LOG.info('Will try again in %d seconds', sleep_time)
                time.sleep(sleep_time)
                sleep_time *= 2
        msg = 'Failed to read s3://%s/%s after %d attempts' \
              % (self.bucket, path, attempts)
        raise TwinDBBackupError(msg)

    def _move_file(self, source, destination):
        s3client = boto3.resource('s3')
        response = s3client.Object(
            self.bucket, destination
        ).copy_from(
            CopySource={
                "Bucket": self.bucket,
                "Key": source
            }
        )
        self.validate_client_response(response)
