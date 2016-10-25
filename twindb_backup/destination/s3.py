import ConfigParser
import glob
import os
from subprocess import Popen, PIPE
import boto3 as boto3
from twindb_backup import log, get_directories_to_backup
from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mysql_source import MySQLSource


class S3Error(DestinationError):
    pass


class S3(BaseDestination):
    def __init__(self, bucket, access_key_id, secret_access_key,
                 default_region='us-east-1'):
        super(S3, self).__init__()
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.default_region = default_region
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_DEFAULT_REGION"] = self.default_region

    def save(self, handler, name, keep_local=None):
        """
        Read from handler and save it to Amazon S3

        :param handler:
        :return: exit code
        :raise: SshError if any error
        """
        try:
            remote_name = "s3://{bucket}/{name}".format(
                bucket=self.bucket,
                name=name
            )

            input_handler = handler

            proc_tee = None

            if keep_local:
                local_name = keep_local + '/' + name
                self._mkdir_p(os.path.dirname(local_name))
                cmd = [
                    'tee',
                    local_name
                ]
                log.debug('Running %s', ' '.join(cmd))
                proc_tee = Popen(cmd, stdin=handler, stdout=PIPE, stderr=PIPE)
                input_handler = proc_tee.stdout

            cmd = ["aws", "s3", "cp", "-", remote_name]
            log.debug('Running %s', ' '.join(cmd))
            proc = Popen(cmd, stdin=input_handler, stdout=PIPE, stderr=PIPE)
            cout, cerr = proc.communicate()

            if proc.returncode:
                if cout:
                    log.info(cout)
                log.error(cerr)

            if proc_tee:
                cout, cerr = proc_tee.communicate()
                if proc_tee.returncode:
                    if cout:
                        log.info(cout)
                    log.error(cerr)

            return proc.returncode

        except S3Error as err:
            log.error(err)

    def _apply_policy_for_files(self, config, run_type):

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)

        backup_dirs = get_directories_to_backup(config)
        for path in backup_dirs:
            src = FileSource(path, run_type)
            prefix = "{prefix}/files/{file}".format(
                prefix=src.get_prefix(),
                file=src.sanitize_filename()
            )
            log.debug('Listing s3://%s/%s',
                      bucket.name, prefix)
            keep_copies = config.getint('retention',
                                        '%s_copies' % run_type)
            objects = sorted(bucket.objects.filter(Prefix=prefix))
            to_delete = objects[:-keep_copies]
            for obj in to_delete:
                log.debug('deleting {0}:{1}'.format(bucket.name, obj.key))
                obj.delete()

            try:
                keep_local_path = config.get('destination', 'keep_local_path')
                dir_backups = "{local_path}/{prefix}/files/{file}*".format(
                    local_path=keep_local_path,
                    prefix=src.get_prefix(),
                    file=src.sanitize_filename()
                )
                self._delete_local_files(dir_backups, keep_copies)

            except ConfigParser.NoOptionError:
                pass

    def _apply_policy_for_mysql(self, config, run_type):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)

        defaults_file = config.get('mysql', 'mysql_defaults_file')
        src = MySQLSource(defaults_file, run_type)

        prefix = "{prefix}/mysql/mysql-".format(
            prefix=src.get_prefix()
        )
        log.debug('Listing s3://%s/%s',
                  bucket.name, prefix)
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)
        objects = sorted(bucket.objects.filter(Prefix=prefix))
        to_delete = objects[:-keep_copies]
        for obj in to_delete:
            log.debug('deleting {0}:{1}'.format(bucket.name, obj.key))
            obj.delete()

        try:
            keep_local_path = config.get('destination', 'keep_local_path')
            dir_backups = "{local_path}/{prefix}/mysql/mysql-*".format(
                local_path=keep_local_path,
                prefix=src.get_prefix()
            )
            self._delete_local_files(dir_backups, keep_copies)

        except ConfigParser.NoOptionError:
            pass

    @staticmethod
    def _delete_local_files(dir_backups, keep_copies):
        local_files = sorted(glob.glob(dir_backups))
        log.debug('Local copies: %r', local_files)

        to_delete = local_files[:-keep_copies]
        for fl in to_delete:
            log.debug('Deleting: %s', fl)
            os.unlink(fl)
