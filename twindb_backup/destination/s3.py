# -*- coding: utf-8 -*-
"""
Module for S3 destination.
"""
import base64

import json
import os
import socket

from contextlib import contextmanager
from multiprocessing import Process
from operator import attrgetter
from urlparse import urlparse

from botocore.exceptions import ClientError
from botocore.client import Config

import boto3
from boto3.s3.transfer import TransferConfig

from twindb_backup import LOG
from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError


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


class S3Error(DestinationError):
    """S3 destination errors"""
    pass


class AWSAuthOptions(object):  # pylint: disable=too-few-public-methods
    """Class to store AWS credentials"""
    def __init__(self, access_key_id, secret_access_key,
                 default_region='us-east-1'):
        self.default_region = default_region
        self.secret_access_key = secret_access_key
        self.access_key_id = access_key_id


class S3(BaseDestination):
    """S3 destination class."""
    def __init__(self, bucket, aws_options, hostname=socket.gethostname()):
        """
        Create instance of S3 destination

        :param bucket: Bucket name
        :type bucket: str
        :param aws_options: AWS credentials
        :type aws_options: AWSAuthOptions
        :param hostname:
        """
        super(S3, self).__init__()
        self.bucket = bucket
        self.remote_path = 's3://{bucket}'.format(bucket=self.bucket)

        self.access_key_id = aws_options.access_key_id
        self.secret_access_key = aws_options.secret_access_key
        self.default_region = aws_options.default_region

        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_DEFAULT_REGION"] = self.default_region

        self.status_path = "{hostname}/status".format(
            hostname=hostname
        )

        # Setup an authenticated S3 client that we will use throughout
        self.s3_client = self.setup_s3_client()

    def setup_s3_client(self):
        """Creates an authenticated s3 client."""
        session = boto3.Session(aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_access_key)
        s3_config = Config(connect_timeout=S3_CONNECT_TIMEOUT,
                           read_timeout=S3_READ_TIMEOUT)
        client = session.client('s3', region_name=self.default_region,
                                config=s3_config)

        return client

    def create_bucket(self):
        """Creates the bucket in s3 that will store the backups.

        :return bool: True on success, False on failure
        """
        bucket_exists = True

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError as err:
            # We come here meaning we did not find the bucket
            try:
                if err.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    bucket_exists = False
            except:
                raise err

        if not bucket_exists:
            LOG.info('Created bucket %s', self.bucket)
            response = self.s3_client.create_bucket(Bucket=self.bucket)
            self.validate_client_response(response)

        return True

    def delete_bucket(self, force=False):
        """Delete the bucket in s3 that was storing the backups.

        :param bool force: If the bucket is non-empty then delete the objects
            before deleting the bucket.
        :return bool: True on success, False on failure
        """
        bucket_exists = True

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError as err:
            # We come here meaning we did not find the bucket
            try:
                if err.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    bucket_exists = False
            except:
                raise err

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
        Delete all objects from S3 bucket

        :return: True if operation was successful
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
        :return: exit code
        """
        try:
            with handler as file_obj:
                ret = self._upload_object(file_obj, name)
                LOG.debug('Returning code %d', ret)
                return ret
        except S3Error as err:
            LOG.error('S3 upload failed: %s', err)
            exit(1)

    def list_files(self, prefix, recursive=False):
        s3client = boto3.resource('s3')
        bucket = s3client.Bucket(self.bucket)

        LOG.debug('Listing %s in bucket %s', prefix, self.bucket)

        norm_prefix = prefix.replace('s3://%s/' % bucket.name, '')
        LOG.debug('norm_prefix = %s', norm_prefix)

        return sorted(bucket.objects.filter(Prefix=norm_prefix),
                      key=attrgetter('key'))

    def find_files(self, prefix, run_type):
        s3client = boto3.resource('s3')
        bucket = s3client.Bucket(self.bucket)
        LOG.debug('Listing %s in bucket %s', prefix, bucket)
        files = []

        try:
            all_objects = bucket.objects.filter(Prefix='')
            for file_object in all_objects:
                if "/" + run_type + "/" in file_object.key:
                    files.append("s3://%s/%s" % (self.bucket, file_object.key))

            return sorted(files)
        except Exception as err:
            LOG.error('Failed to list objects in bucket %s: %s',
                      self.bucket, err)
            raise

    def delete(self, obj):
        """Deletes a s3 object.

        :param S3.Object obj: The s3 object to delete.
        :return bool: True on success, False on failure
        """
        s3client = boto3.resource('s3')
        bucket = s3client.Bucket(self.bucket)

        LOG.debug('deleting s3://%s/%s', bucket.name, obj.key)

        return obj.delete()

    @contextmanager
    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination.
        :return:
        """
        object_key = urlparse(path).path.lstrip('/')

        def _download_object(s3_client, bucket_name, key, read_fd, write_fd):
            # The read end of the pipe must be closed in the child process
            # before we start writing to it.
            os.close(read_fd)

            with os.fdopen(write_fd, 'wb') as w_pipe:
                s3_client.download_fileobj(bucket_name, key, w_pipe)

        LOG.debug('Fetching object %s from bucket %s',
                  object_key,
                  self.bucket)

        read_pipe, write_pipe = os.pipe()

        download_proc = Process(target=_download_object,
                                args=(self.s3_client, self.bucket,
                                      object_key, read_pipe, write_pipe))
        download_proc.start()

        # The write end of the pipe must be closed in this process before
        # we start reading from it.
        os.close(write_pipe)
        yield read_pipe

        LOG.debug('Successfully streamed %s', path)

        if download_proc:
            download_proc.join()

    def _upload_object(self, file_obj, object_key):
        """Upload objects to S3 in streaming fashion.

        :param file file_obj: A file like object to upload. At a minimum, it
            must implement the read method, and must return bytes.
        :param str object_key: The destination key where to upload the object.
        """
        remote_name = "s3://{bucket}/{name}".format(
            bucket=self.bucket,
            name=object_key
        )

        LOG.debug("Generating S3 transfer config")
        s3_transfer_config = self.get_transfer_config()

        LOG.debug("Starting to stream to %s", remote_name)
        self.s3_client.upload_fileobj(file_obj, self.bucket, object_key,
                                      Config=s3_transfer_config)
        LOG.debug("Successfully streamed to %s", remote_name)

        return self._validate_upload(object_key)

    def _validate_upload(self, object_key):
        """Validates that upload of an object was successful. Raises an
            exception if the response code is not 200"""
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

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))

        response = self.s3_client.put_object(Body=raw_status,
                                             Bucket=self.bucket,
                                             Key=self.status_path)

        self.validate_client_response(response)

        return status

    def _read_status(self):

        if self._status_exists():
            response = self.s3_client.get_object(Bucket=self.bucket,
                                                 Key=self.status_path)
            self.validate_client_response(response)

            content = response['Body'].read()
            return json.loads(base64.b64decode(content))
        else:
            return self._empty_status

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

    @staticmethod
    def validate_client_response(response):
        """Validates the response returned by the client. Raises an exception
            if the response code is not 200 or 204

        :param dict response: The response that needs to be validated
        """
        try:
            http_status_code = response['ResponseMetadata']['HTTPStatusCode']
        except KeyError as err:
            raise S3Error('S3 client returned invalid response: %s' % err)

        if http_status_code not in [200, 204]:
            raise S3Error('S3 client returned error code: %s' %
                          http_status_code)

    @staticmethod
    def get_transfer_config():
        """
        Build Transfer config

        :return: Transfer config
        :rtype: TransferConfig
        """
        transfer_config = TransferConfig(
            multipart_threshold=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_concurrency=S3_UPLOAD_CONCURRENCY,
            multipart_chunksize=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_io_queue=S3_UPLOAD_IO_QUEUE_SIZE,
            io_chunksize=S3_UPLOAD_IO_CHUNKS_SIZE_BYTES)

        return transfer_config
