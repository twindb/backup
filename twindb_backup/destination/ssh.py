import ConfigParser
import glob
import os
from subprocess import Popen, PIPE
from twindb_backup import log, get_directories_to_backup
from twindb_backup.destination.base_destination import BaseDestination, \
    DestinationError
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mysql_source import MySQLSource


class SshError(DestinationError):
    pass


class Ssh(BaseDestination):
    def __init__(self, host='127.0.0.1', port=22, key='/root/.id_rsa',
                 user='root',
                 remote_path=None):
        super(Ssh, self).__init__()
        self.remote_path = remote_path
        self.user = user
        self.key = key
        self.port = port
        self.host = host
        self._ssh_command = ['ssh', '-l', self.user,
                             '-o',
                             'StrictHostKeyChecking=no',
                             '-o',
                             'PasswordAuthentication=no',
                             '-p', str(self.port),
                             '-i', key,
                             self.host]

    def save(self, handler, name, keep_local=None):
        """
        Read from handler and save it on remote ssh server

        :param handler:
        :return: exit code
        :raise: SshError if any error
        """
        try:
            remote_name = self.remote_path + '/' + name
            self._mkdir_r(os.path.dirname(remote_name))

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

            cmd = self._ssh_command + ["cat - > \"%s\"" % remote_name]
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

        except SshError as err:
            log.error(err)

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :return: exit code
        """
        cmd = self._ssh_command + ["mkdir -p \"%s\"" % path]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        if proc.returncode:
            raise SshError('Failed to create directory %s: %s' % (path, cerr))

        return proc.returncode

    def _apply_policy_for_files(self, config, run_type):

        backup_dirs = get_directories_to_backup(config)
        for path in backup_dirs:
            src = FileSource(path, run_type)
            prefix = "{remote_path}/{prefix}/files/{file}".format(
                remote_path=self.remote_path,
                prefix=src.get_prefix(),
                file=src.sanitize_filename()
            )
            log.debug('Listing ssh://%s', prefix)
            keep_copies = config.getint('retention',
                                        '%s_copies' % run_type)

            cmd = self._ssh_command + ["ls %s*" % prefix]
            log.debug('Running %s', ' '.join(cmd))
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            cout, cerr = proc.communicate()

            objects = sorted(cout.split())
            to_delete = objects[:-keep_copies]
            for obj in to_delete:
                log.debug('deleting %s', obj)
                self._delete_file(obj)

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

        defaults_file = config.get('mysql', 'mysql_defaults_file')
        src = MySQLSource(defaults_file, run_type)

        prefix = "{remote_path}/{prefix}/mysql/mysql-".format(
            remote_path=self.remote_path,
            prefix=src.get_prefix()
        )
        log.debug('Listing ssh://%s', prefix)
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)
        cmd = self._ssh_command + ["ls %s*" % prefix]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        objects = sorted(cout.split())
        to_delete = objects[:-keep_copies]
        for obj in to_delete:
            log.debug('deleting %s', obj)
            self._delete_file(obj)

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

    def _delete_file(self, obj):
        cmd = self._ssh_command + ["rm %s" % obj]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd)
        proc.communicate()
