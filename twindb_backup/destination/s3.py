import base64
from contextlib import contextmanager
import json
import os
import socket
import boto3 as boto3
from subprocess import Popen, PIPE
from botocore.exceptions import ClientError
from twindb_backup import log
from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError


class S3Error(DestinationError):
    pass


class S3(BaseDestination):
    def __init__(self, bucket, access_key_id, secret_access_key,
                 default_region='us-east-1'):
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
            hostname=socket.gethostname()
        )

    def save(self, handler, name, keep_local=None):
        """
        Read from handler and save it to Amazon S3

        :param keep_local: save backup copy in this directory
        :param name: save backup copy in a file with this name
        :param handler: stdout handler from backup source
        :return: exit code
        """
        remote_name = "s3://{bucket}/{name}".format(
            bucket=self.bucket,
            name=name
        )
        cmd = ["aws", "s3", "cp", "-", remote_name]
        return self._save(cmd, handler, keep_local, name)

    def list_files(self, prefix, recursive=False):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        log.debug('Listing s3://%s/%s', bucket.name, prefix)
        return sorted(bucket.objects.filter(Prefix=prefix))

    def find_files(self, prefix):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        log.debug('Listing s3://%s/%s', bucket.name, prefix)
        files = ["s3://%s/%s" % (self.bucket, f.key)
                 for f in sorted(bucket.objects.filter(Prefix=prefix))]
        return files

    def delete(self, obj):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        log.debug('deleting {0}:{1}'.format(bucket.name, obj.key))
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
