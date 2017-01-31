import os
import shlex
import tempfile
import time

from ConfigParser import NoOptionError
from contextlib import contextmanager
from pymysql.err import InternalError
from subprocess import Popen, PIPE
from twindb_backup import log, get_files_to_delete
from twindb_backup.source.base_source import BaseSource
from twindb_backup.util import get_connection


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
        cmd = [
            "innobackupex",
            "--defaults-file=%s" % self.defaults,
            "--stream=xbstream",
            "--host=127.0.0.1"
            ]

        if self.is_galera():
            cmd.append("--galera-info")
            cmd.append("--no-backup-locks")

        if self.full:
            cmd.append(".")
        else:
            cmd += [
                "--incremental",
                ".",
                "--incremental-lsn=%d" % self.parent_lsn
            ]

        # If this is a Galera node then additional step needs to be taken to
        # prevent the backups from locking up the cluster.
        wsrep_desynced = False
        try:
            if self.is_galera():
                wsrep_desynced = self.enable_wsrep_desync()

            log.debug('Running %s', ' '.join(cmd))
            stderr_file = tempfile.NamedTemporaryFile(delete=False)
            proc_innobackupex = Popen(cmd,
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
        finally:
            if wsrep_desynced:
                self.disable_wsrep_desync()

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return self._get_name('mysql')

    def apply_retention_policy(self, dst, config, run_type, status):

        prefix = "{remote_path}/{prefix}/mysql/mysql-".format(
            remote_path=dst.remote_path,
            prefix=self.get_prefix()
        )
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)

        objects = dst.list_files(prefix)

        for fl in get_files_to_delete(objects, keep_copies):
            log.debug('Deleting remote file %s' % fl)
            dst.delete(fl)
            status = self._delete_from_status(status, dst.remote_path, fl)

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
        try:
            ref_filename = fl.key
        except AttributeError:
            prefix = prefix.rstrip('/')
            ref_filename = str(fl).replace(prefix + '/', '', 1)

        result_status = status
        try:
            del(result_status[self.run_type][ref_filename])
        except KeyError:
            pass
        return result_status

    def enable_wsrep_desync(self):
        """
        Try to enable wsrep_desync

        :return: True if wsrep_desync was enabled. False if not supported
        """
        try:
            with get_connection(host='127.0.0.1',
                                defaults_file=self.defaults) as db:
                with db.cursor() as cursor:
                    cursor.execute('SET GLOBAL wsrep_desync=ON')
            return True
        except Exception as e:
            log.debug(e)
            return False

    def disable_wsrep_desync(self):
        """
        Wait till wsrep_local_recv_queue is zero
        and disable wsrep_local_recv_queue then
        """
        max_time = time.time() + 900
        try:
            with get_connection(host='127.0.0.1',
                                defaults_file=self.defaults) as db:
                with db.cursor() as cursor:
                    while time.time() < max_time:
                        cursor.execute("SHOW GLOBAL STATUS LIKE "
                                       "'wsrep_local_recv_queue'")

                        res = {r['Variable_name'].lower(): r['Value'].lower()
                               for r in cursor.fetchall()}

                        if not res.get('wsrep_local_recv_queue'):
                            raise Exception('Unknown status variable '
                                            '"wsrep_local_recv_queue"')

                        if int(res['wsrep_local_recv_queue']) == 0:
                            break

                        time.sleep(1)

                    log.debug('Disabling wsrep_desync')
                    cursor.execute("SET GLOBAL wsrep_desync=OFF")
        except Exception as e:
            log.error(e)

    @staticmethod
    def get_my_cnf():
        mysql_configs = [
            '/etc/my.cnf',
            '/etc/mysql/my.cnf'
        ]
        for cnf in mysql_configs:
            try:
                with open(cnf) as fp:
                    yield cnf, fp.read()
            except IOError:
                continue

    @property
    def wsrep_provider_version(self):
        with get_connection(host='127.0.0.1',
                            defaults_file=self.defaults) as db:
            with db.cursor() as cursor:
                cursor.execute("SHOW STATUS LIKE 'wsrep_provider_version'")

                res = {row['Variable_name'].lower(): row['Value'].lower()
                       for row in cursor.fetchall()}

        if res.get('wsrep_provider_version'):
            return res['wsrep_provider_version'].split('(')[0]

        return None

    @property
    def galera(self):
        return self.is_galera()

    def is_galera(self):
        try:
            with get_connection(host='127.0.0.1',
                                defaults_file=self.defaults) as db:
                with db.cursor() as cursor:
                    cursor.execute("SELECT @@wsrep_on as wsrep_on")
                    row = cursor.fetchone()

                    return (str(row['wsrep_on']).lower() == "1" or
                            row['wsrep_on'].lower() == 'on')
        except InternalError as err:
            error_code, error_message = err.args

            if error_code == 1193:
                log.debug('Galera is not supported or not enabled')
                return False
            else:
                raise
