import base64
import boto3 as boto3
import json
import os
import socket

from botocore.client import Config
from contextlib import contextmanager
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from multiprocessing import Process
from operator import attrgetter
from twindb_backup import log
from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError
from urlparse import urlparse


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
    pass


class S3(BaseDestination):
    def __init__(self, bucket, access_key_id, secret_access_key,
                 default_region='us-east-1', hostname=socket.gethostname()):
        super(S3, self).__init__()
        self.bucket = bucket
        self.remote_path = 's3://{bucket}'.format(bucket=self.bucket)
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.default_region = default_region
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
        except ClientError as e:
            # We come here meaning we did not find the bucket
            try:
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    bucket_exists = False
            except:
                raise e

        if not bucket_exists:
            log.info('Created bucket %s' % self.bucket)
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
        except ClientError as e:
            # We come here meaning we did not find the bucket
            try:
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    bucket_exists = False
            except:
                raise e

        if bucket_exists:
            log.info('Deleting bucket %s' % self.bucket)

            if force:
                log.info('Deleting the objects in the bucket %s' % self.bucket)
                self.delete_all_objects()

            response = self.s3_client.delete_bucket(Bucket=self.bucket)
            self.validate_client_response(response)

            log.info('Bucket %s successfully deleted' % self.bucket)

        return True

    def delete_all_objects(self):
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
                return self._upload_object(file_obj, name)
        except S3Error as e:
            log.error('S3 upload failed: %s' % e)
            exit(1)

    def list_files(self, prefix, recursive=False):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)

        log.debug('Listing %s', prefix)

        norm_prefix = prefix.replace('s3://%s/' % bucket.name, '')
        log.debug('norm_prefix = %s' % norm_prefix)

        return sorted(bucket.objects.filter(Prefix=norm_prefix),
                      key=attrgetter('key'))

    def find_files(self, prefix, run_type):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        log.debug('Listing %s', prefix)
        files = []

        try:
            all_objects = bucket.objects.filter(Prefix='')
            for f in all_objects:
                if "/" + run_type + "/" in f.key:
                    files.append("s3://%s/%s" % (self.bucket, f.key))

            return sorted(files)
        except Exception as e:
            log.error('Failed to list objects in bucket %s: %s' %
                      (self.bucket, e))
            raise

    def delete(self, obj):
        """Deletes a s3 object.

        :param S3.Object obj: The s3 object to delete.
        :return bool: True on success, False on failure
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)

        log.debug('deleting s3://{0}/{1}'.format(bucket.name, obj.key))

        return obj.delete()

    @contextmanager
    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination.
        :return:
        """
        object_key = urlparse(path).path.lstrip('/')
        download_proc = None

        def download_object(s3_client, bucket_name, key, read_fd, write_fd):
            # The read end of the pipe must be closed in the child process
            # before we start writing to it.
            os.close(read_fd)

            with os.fdopen(write_fd, 'wb') as write_pipe:
                s3_client.download_fileobj(bucket_name, key, write_pipe)

        try:
            log.debug('Fetching object %s from bucket %s' %
                      (object_key, self.bucket))

            read_pipe, write_pipe = os.pipe()

            download_proc = Process(target=download_object,
                                    args=(self.s3_client, self.bucket,
                                          object_key, read_pipe, write_pipe))
            download_proc.start()

            # The write end of the pipe must be closed in this process before
            # we start reading from it.
            os.close(write_pipe)
            yield read_pipe

            log.debug('Successfully streamed %s' % path)
        except Exception as e:
            log.error('Failed to read from %s: %s' % (path, e))
            exit(1)
        finally:
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

        log.debug("Generating S3 transfer config")
        s3_transfer_config = self.get_transfer_config()

        log.debug("Starting to stream to %s" % remote_name)
        self.s3_client.upload_fileobj(file_obj, self.bucket, object_key,
                                      Config=s3_transfer_config)
        log.debug("Successfully streamed to %s" % remote_name)

        return self._validate_upload(object_key)

    def _validate_upload(self, object_key):
        """Validates that upload of an object was successful. Raises an
            exception if the response code is not 200"""
        remote_name = "s3://{bucket}/{name}".format(
            bucket=self.bucket,
            name=object_key
        )

        log.debug("Validating upload to %s" % remote_name)

        response = self.s3_client.get_object(Bucket=self.bucket,
                                             Key=object_key)
        self.validate_client_response(response)

        log.debug("Upload successfully validated")

        return True

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))

        response = self.s3_client.put_object(Body=raw_status,
                                             Bucket=self.bucket,
                                             Key=self.status_path)

        self.validate_client_response(response)

        return status

    def _read_status(self):
        if not self._status_exists():
            return self._empty_status
        else:
            response = self.s3_client.get_object(Bucket=self.bucket,
                                                 Key=self.status_path)
            self.validate_client_response(response)

            content = response['Body'].read()
            return json.loads(base64.b64decode(content))

    def _status_exists(self):
        s3 = boto3.resource('s3')
        status_object = s3.Object(self.bucket, self.status_path)
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
        except Exception as e:
            raise S3Error('S3 client returned invalid response: %s' % e)

        if http_status_code not in [200, 204]:
            raise S3Error('S3 client returned error code: %s' %
                          http_status_code)

    @staticmethod
    def get_transfer_config():
        transfer_config = TransferConfig(
            multipart_threshold=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_concurrency=S3_UPLOAD_CONCURRENCY,
            multipart_chunksize=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_io_queue=S3_UPLOAD_IO_QUEUE_SIZE,
            io_chunksize=S3_UPLOAD_IO_CHUNKS_SIZE_BYTES)

        return transfer_config
