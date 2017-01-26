import base64
from contextlib import contextmanager
import json
from operator import attrgetter
import os
import socket
import boto3 as boto3

from botocore.client import Config
from subprocess import Popen, PIPE
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from twindb_backup import log
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
        self._s3_client = self.setup_s3_client()

    def setup_s3_client(self):
        """Creates an authenticated s3 client."""
        session = boto3.Session(aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_access_key)
        s3_config = Config(connect_timeout=S3_CONNECT_TIMEOUT,
                           read_timeout=S3_READ_TIMEOUT)
        client = session.client('s3', region_name=self.default_region,
                                config=s3_config)

        # Now we make a call to confirm that the client is authenticated,
        # otherwise boto3 doesn't tell us if the client is actually connected
        try:
            client.list_buckets()
        except ClientError as e:
            log.error('S3 Client could not connect to the region %s: %s' %
                      (self.default_region, e))
            exit(1)

        return client

    def save(self, handler, name, keep_local=None):
        """
        Read from handler and save it to Amazon S3

        :param keep_local: save backup copy in this directory
        :param name: save backup copy in a file with this name
        :param handler: stdout handler from backup source
        :return: exit code
        """
        try:
            return self._upload_object(handler, name)
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

        all_objects = bucket.objects.filter(Prefix='')
        for f in all_objects:
            if "/" + run_type + "/" in f.key:
                files.append("s3://%s/%s" % (self.bucket, f.key))

        return sorted(files)

    def delete(self, obj):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        log.debug('deleting s3://{0}/{1}'.format(bucket.name, obj.key))
        obj.delete()

    @contextmanager
    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination
        :return:
        """
        cmd = ["aws", "s3", "cp", path, "-"]
        try:
            log.debug('Running %s', " ".join(cmd))
            proc = Popen(cmd, stderr=PIPE, stdout=PIPE)

            yield proc.stdout

            cout, cerr = proc.communicate()
            if proc.returncode:
                log.error('Failed to read from %s: %s' % (path, cerr))
                exit(1)
            else:
                log.debug('Successfully streamed %s', path)

        except OSError as err:
            log.error('Failed to run %s: %s', cmd, err)
            exit(1)

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
        self._s3_client.upload_fileobj(file_obj, self.bucket, object_key,
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
        response = self._s3_client.get_object(Bucket=self.bucket,
                                              Key=object_key)

        try:
            http_status_code = response['ResponseMetadata']['HTTPStatusCode']
        except Exception as e:
            raise S3Error('Invalid response: %s' % e)

        if http_status_code != 200:
            raise S3Error('Error code: %s' % http_status_code)

        log.debug("Upload successfully validated")

        return True

    @staticmethod
    def get_transfer_config():
        transfer_config = TransferConfig(
            multipart_threshold=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_concurrency=S3_UPLOAD_CONCURRENCY,
            multipart_chunksize=S3_UPLOAD_CHUNK_SIZE_BYTES,
            max_io_queue=S3_UPLOAD_IO_QUEUE_SIZE,
            io_chunksize=S3_UPLOAD_IO_CHUNKS_SIZE_BYTES)

        return transfer_config

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))

        s3 = boto3.resource('s3')
        status_object = s3.Object(self.bucket, self.status_path)
        status_object.put(Body=raw_status)

        return status

    def _read_status(self):
        if not self._status_exists():
            return self._empty_status
        else:
            s3 = boto3.resource('s3')
            status_object = s3.Object(self.bucket, self.status_path)
            content = status_object.get()['Body'].read()
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
