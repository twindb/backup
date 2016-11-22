from ConfigParser import NoOptionError
from contextlib import contextmanager
import os
import shlex
from subprocess import Popen, PIPE
import tempfile
from twindb_backup import log, get_files_to_delete
from twindb_backup.source.base_source import BaseSource


class MySQLSourceError(Exception):
    """Errors during backups"""


class MySQLSource(BaseSource):
    def __init__(self, defaults_file, run_type, config, dst):
        self.defaults = defaults_file
        self._suffix = 'xbstream.gz'
        self._media_type = 'mysql'
        self.lsn = None
        self.binlog_coordinate = None
        self.config = config
        self.dst = dst
        super(MySQLSource, self).__init__(run_type)

    @contextmanager
    def get_stream(self):
        """
        Get a PIPE handler with content of the source
        :return:
        """
        cmd_common = "innobackupex --defaults-file=%s --stream xbstream " \
                     "--host 127.0.0.1 " \
                     % self.defaults
        if self.full:
            cmd = cmd_common + " ."
        else:
            cmd = cmd_common + "--incremental . --incremental-lsn=%d" \
                               % self.parent_lsn

        try:
            log.debug('Running %s', cmd)
            stderr_file = tempfile.NamedTemporaryFile(delete=False)
            proc_innobackupex = Popen(shlex.split(cmd),
                                      stderr=stderr_file,
                                      stdout=PIPE)
            cmd = "gzip -c -"
            try:
                log.debug('Running %s', cmd)
                proc_gzip = Popen(shlex.split(cmd),
                                  stdin=proc_innobackupex.stdout,
                                  stderr=PIPE, stdout=PIPE)
                yield proc_gzip.stdout

                cout, cerr = proc_gzip.communicate()
                if proc_gzip.returncode:
                    log.error('Failed to compress innobackupex stream: '
                              '%s' % cerr)
                    exit(1)
                else:
                    log.debug('Successfully compressed innobackupex stream')

            except OSError as err:
                log.error('Failed to run %s: %s', cmd, err)
                exit(1)

            proc_innobackupex.communicate()
            if proc_innobackupex.returncode:
                log.error('Failed to run innobackupex. '
                          'Check error output in %s', stderr_file.name)
                exit(1)
            else:
                log.debug('Successfully streamed innobackupex output')
            log.debug('innobackupex error log file %s', stderr_file.name)
            self.lsn = self.get_lsn(stderr_file.name)
            self.binlog_coordinate = self.get_binlog_coordinates(
                stderr_file.name
            )
            os.unlink(stderr_file.name)

        except OSError as err:
            log.error('Failed to run %s: %s', cmd, err)
            exit(1)

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return self._get_name('mysql')

    def apply_retention_policy(self, dst, config, run_type, status):

        if dst.remote_path:
            remote_path = dst.remote_path + '/'
        else:
            remote_path = ''

        prefix = "{remote_path}{prefix}/mysql/mysql-".format(
            remote_path=remote_path,
            prefix=self.get_prefix()
        )
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)

        objects = dst.list_files(prefix)

        for fl in get_files_to_delete(objects, keep_copies):
            log.debug('Deleting remote file %s' % fl)
            dst.delete(fl)
            status = self._delete_from_status(status, remote_path, fl)

        self._delete_local_files('mysql', config)

        return status

    @staticmethod
    def get_binlog_coordinates(err_log):
        with open(err_log) as f:
            for line in f:
                if line.startswith('MySQL binlog position:'):
                    filename = line.split()[4].strip(",'")
                    position = int(line.split()[6].strip(",'"))
                    return filename, position
        return None, None

    @staticmethod
    def get_lsn(err_log):
        """Find LSN up to which the backup is taken

        :param err_log: path to Innobackupex error log
        :return: lsn
        """
        with open(err_log) as f:
            for line in f:
                pattern = 'xtrabackup: ' \
                          'The latest check point (for incremental):'
                if line.startswith(pattern):
                    lsn = line.split()[7].strip("'")
                    return int(lsn)
        raise MySQLSourceError('Could not find LSN'
                               ' in XtraBackup error output %s' % err_log)

    @property
    def full(self):
        return self._get_backup_type() == 'full'

    @property
    def incremental(self):
        return not self.full

    def _get_backup_type(self):
        """Return backup type to take. If full_backup=daily then
        for hourly backups it will be incremental, for all other - full

        :return: "full" or "incremental"
        """
        try:
            full_backup = self.config.get('mysql', 'full_backup')
            if self._intervals.index(full_backup) <= \
                    self._intervals.index(self.run_type):
                return "full"
            elif not self._parent_exists():
                return "full"
            else:
                return "incremental"
        except (NoOptionError, ValueError):
            return 'full'

    @property
    def type(self):
        return self._get_backup_type()

    @property
    def status(self):
        return self.dst.status()

    @property
    def parent(self):
        full_backup = self.config.get('mysql', 'full_backup')
        return sorted(self.status[full_backup].keys(), reverse=True)[0]

    @property
    def parent_lsn(self):
        full_backup = self.config.get('mysql', 'full_backup')
        return self.status[full_backup][self.parent]['lsn']

    def _parent_exists(self):
        full_backup = self.config.get('mysql', 'full_backup')
        full_backup_index = self._intervals.index(full_backup)
        for i in xrange(full_backup_index, len(self._intervals)):
            if len(self.dst.status()[self._intervals[i]]) > 0:
                return True

        return False

    def _delete_from_status(self, status, prefix, fl):
        log.debug('status = %r' % status)
        log.debug('prefix = %s' % prefix)
        log.debug('file   = %s' % fl)
        prefix = prefix.rstrip('/')
        ref_filename = str(fl).replace(prefix + '/', '', 1)
        result_status = status
        try:
            del(result_status[self.run_type][ref_filename])
        except KeyError:
            pass
        return result_status
