# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up local MySQL.
"""
import os
import tempfile
import time

from ConfigParser import NoOptionError
from contextlib import contextmanager
from subprocess import PIPE

import pymysql
from spur import LocalShell

from twindb_backup import LOG, get_files_to_delete, INTERVALS
from twindb_backup.source.base_source import BaseSource


class MySQLSourceError(Exception):
    """Errors during backups"""


class MySQLConnectInfo(object):  # pylint: disable=too-few-public-methods
    """MySQL connection details """
    def __init__(self, defaults_file,
                 connect_timeout=10,
                 cursor=pymysql.cursors.DictCursor,
                 hostname="127.0.0.1"):

        self.cursor = cursor
        self.connect_timeout = connect_timeout
        self.defaults_file = defaults_file
        self.hostname = hostname


class MySQLSource(BaseSource):
    """MySQLSource class"""
    def __init__(self, mysql_connect_info, run_type, full_backup, dst):
        """
        MySQLSource constructor

        :param mysql_connect_info: MySQL connection details
        :type mysql_connect_info: MySQLConnectInfo
        :param run_type:
        :param full_backup: When to do full backup e.g. daily, weekly
        :type full_backup: str
        :param dst:
        """

        class _BackupInfo(object):  # pylint: disable=too-few-public-methods
            """class to store details about backup copy"""
            def __init__(self, lsn=None,
                         binlog_coordinate=None):
                self.lsn = lsn
                self.binlog_coordinate = binlog_coordinate

        # MySQL
        if not isinstance(mysql_connect_info, MySQLConnectInfo):
            raise MySQLSourceError('mysql_connect_info must be '
                                   'instance of MySQLConnectInfo')

        self._connect_info = mysql_connect_info

        self._backup_info = _BackupInfo()
        if full_backup not in INTERVALS:
            raise MySQLSourceError('full_backup must be one of %r. '
                                   'Got %r instead.'
                                   % (INTERVALS, full_backup))

        if run_type not in INTERVALS:
            raise MySQLSourceError('run_type must be one of %r. '
                                   'Got %r instead.'
                                   % (INTERVALS, run_type))

        self.suffix = 'xbstream'
        self._media_type = 'mysql'
        self.full_backup = full_backup
        self.dst = dst
        super(MySQLSource, self).__init__(run_type)

    @property
    def binlog_coordinate(self):
        """
        Binary log coordinate up to that backup is taken

        :return: file name and position
        :rtype: tuple
        """
        return self._backup_info.binlog_coordinate

    @property
    def lsn(self):
        """
        The latest LSN of the taken backup
        :return: LSN
        :rtype: int
        """
        return self._backup_info.lsn

    @contextmanager
    def get_stream(self):
        """
        Get a PIPE handler with content of the source
        :return:
        """
        cmd = self._prepare_stream_cmd()
        # If this is a Galera node then additional step needs to be taken to
        # prevent the backups from locking up the cluster.
        wsrep_desynced = False
        LOG.debug('Running %s', ' '.join(cmd))
        stderr_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            if self.is_galera():
                wsrep_desynced = self.enable_wsrep_desync()

            shell = LocalShell()
            LOG.debug('Running %s', ' '.join(cmd))

            proc_innobackupex = shell.spawn(cmd,
                                            stderr=stderr_file,
                                            allow_error=True)
            result = proc_innobackupex.wait_for_result()

            yield result.output

            if result.returncode:
                LOG.error('Failed to run innobackupex. '
                          'Check error output in %s', stderr_file.name)
                self.dst.delete(self.get_name())
                exit(1)
            else:
                LOG.debug('Successfully streamed innobackupex output')
            self._update_backup_info(stderr_file)
        except OSError as err:
            LOG.error('Failed to run %s: %s', cmd, err)
            exit(1)
        finally:
            if wsrep_desynced:
                self.disable_wsrep_desync()

    def _handle_failure_exec(self, err, stderr_file):
        """Cleanup on failure exec"""
        LOG.error(err)
        LOG.error('Failed to run innobackupex. '
                  'Check error output in %s', stderr_file.name)
        self.dst.delete(self.get_name())
        exit(1)

    def _update_backup_info(self, stderr_file):
        """Update backup_info from stderr"""

        LOG.debug('innobackupex error log file %s', stderr_file.name)
        self._backup_info.lsn = self.get_lsn(stderr_file.name)
        self._backup_info.binlog_coordinate = self.get_binlog_coordinates(
            stderr_file.name
        )
        os.unlink(stderr_file.name)

    def _prepare_stream_cmd(self):
        """Prepare command for get stream"""

        cmd = [
            "innobackupex",
            "--defaults-file=%s" % self._connect_info.defaults_file,
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

        return cmd

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return self._get_name('mysql')

    def apply_retention_policy(self, dst, config, run_type, status):
        """
        Delete old backup copies.

        :param dst: Destination where the backups are stored.
        :type dst: BaseDestination
        :param config: Tool configuration
        :type config: ConfigParser.ConfigParser
        :param run_type: Run type.
        :type run_type: str
        :param status: Backups status.
        :type status: dict
        :return: Updated status.
        :rtype: dict
        """

        prefix = "{remote_path}/{prefix}/mysql/mysql-".format(
            remote_path=dst.remote_path,
            prefix=self.get_prefix()
        )
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)

        objects = dst.list_files(prefix)

        for backup_copy in get_files_to_delete(objects, keep_copies):
            LOG.debug('Deleting remote file %s', backup_copy)
            dst.delete(backup_copy)
            status = self._delete_from_status(status,
                                              dst.remote_path,
                                              backup_copy)

        self._delete_local_files('mysql', config)

        return status

    @staticmethod
    def get_binlog_coordinates(err_log_path):
        """
        Parse innobackupex log and return binary log coordinate

        :param err_log_path: path to the innobackupex log
        :type err_log_path: str
        :return: Binlog coordinate.
        :rtype: tuple
        """
        with open(err_log_path) as error_log:
            for line in error_log:
                if line.startswith('MySQL binlog position:'):
                    filename = line.split()[4].strip(",'")
                    position = int(line.split()[6].strip(",'"))
                    return filename, position
        return None, None

    @staticmethod
    def get_lsn(err_log_path):
        """Find LSN up to which the backup is taken

        :param err_log_path: path to Innobackupex error log
        :return: lsn
        :rtype: int
        """
        with open(err_log_path) as error_log:
            for line in error_log:
                pattern = 'xtrabackup: ' \
                          'The latest check point (for incremental):'
                if line.startswith(pattern):
                    lsn = line.split()[7].strip("'")
                    return int(lsn)
        raise MySQLSourceError('Could not find LSN'
                               ' in XtraBackup error output %s'
                               % err_log_path)

    @property
    def full(self):
        """
        Check if the backup copy is a full copy.

        :return: True if it's a full copy.
        :rtype: bool
        """
        return self._get_backup_type() == 'full'

    @property
    def incremental(self):
        """
        Check if the backup copy is an incremental copy.

        :return: True if it's an incremental copy.
        :rtype: bool
        """
        return not self.full

    def _get_backup_type(self):
        """Return backup type to take. If full_backup=daily then
        for hourly backups it will be incremental, for all other - full

        :return: "full" or "incremental"
        """
        try:
            if self._intervals.index(self.full_backup) <= \
                    self._intervals.index(self.run_type):
                return "full"
            elif not self._full_copy_exists():
                return "full"
            else:
                return "incremental"
        except (NoOptionError, ValueError):
            return 'full'

    @property
    def type(self):
        """Get backup copy type - full or incremental

        :return: 'full' or 'incrmental'
        :rtype: str
        """
        return self._get_backup_type()

    @property
    def status(self):
        """Backup status on a destination

        :return: Backups status
        :rtype: dict
        """
        return self.dst.status()

    @property
    def parent(self):
        """
        Get name of the parent copy.

        :return: backup name of the parent copy
            or its own name if the copy is a full copy.
        :rtype: str
        """
        return sorted(self.status[self.full_backup].keys(), reverse=True)[0]

    @property
    def parent_lsn(self):
        """LSN of the parent backup copy.

        :return: LSN of the parent or its own LSN
            if the backup copy is a full copy.
        """
        return self.status[self.full_backup][self.parent]['lsn']

    def _full_copy_exists(self):
        full_backup_index = self._intervals.index(self.full_backup)
        for i in xrange(full_backup_index, len(self._intervals)):
            if len(self.dst.status()[self._intervals[i]]) > 0:
                return True

        return False

    def _delete_from_status(self, status, prefix, backup_copy):
        LOG.debug('status = %r', status)
        LOG.debug('prefix = %s', prefix)
        LOG.debug('file   = %s', backup_copy)
        try:
            ref_filename = backup_copy.key
        except AttributeError:
            prefix = prefix.rstrip('/')
            ref_filename = str(backup_copy).replace(prefix + '/', '', 1)

        result_status = status
        try:
            del result_status[self.run_type][ref_filename]
        except KeyError:
            pass
        return result_status

    def enable_wsrep_desync(self):
        """
        Try to enable wsrep_desync

        :return: True if wsrep_desync was enabled. False if not supported
        """
        try:
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SET GLOBAL wsrep_desync=ON')
            return True
        except pymysql.Error as err:
            LOG.debug(err)
            return False

    def disable_wsrep_desync(self):
        """
        Wait till wsrep_local_recv_queue is zero
        and disable wsrep_local_recv_queue then
        """
        max_time = time.time() + 900
        try:
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
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

                    LOG.debug('Disabling wsrep_desync')
                    cursor.execute("SET GLOBAL wsrep_desync=OFF")
        except pymysql.Error as err:
            LOG.error(err)

    @staticmethod
    def get_my_cnf():
        """Get generator that spits out my.cnf files

        :return: File name and content of MySQL config file.
        :rtype: tuple
        """
        mysql_configs = [
            '/etc/my.cnf',
            '/etc/mysql/my.cnf'
        ]
        for cnf_parg in mysql_configs:
            try:
                with open(cnf_parg) as my_cnf:
                    yield cnf_parg, my_cnf.read()
            except IOError:
                continue

    @property
    def wsrep_provider_version(self):
        """
        Parse Galera version from wsrep_provider_version.

        :return: Galera version
        :rtype: str
        """
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW STATUS LIKE 'wsrep_provider_version'")

                res = {row['Variable_name'].lower(): row['Value'].lower()
                       for row in cursor.fetchall()}

        if res.get('wsrep_provider_version'):
            return res['wsrep_provider_version'].split('(')[0]

        return None

    @property
    def galera(self):
        """Check if local MySQL instance is a Galera cluster

        :return: True if it's a Galera.
        :rtype: bool
        """
        return self.is_galera()

    def is_galera(self):
        """Check if local MySQL instance is a Galera cluster

        :return: True if it's a Galera.
        :rtype: bool
        """
        try:
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT @@wsrep_on as wsrep_on")
                    row = cursor.fetchone()

                    return (str(row['wsrep_on']).lower() == "1" or
                            row['wsrep_on'].lower() == 'on')
        except pymysql.InternalError as err:
            error_code, error_message = err.args

            if error_code == 1193:
                LOG.debug('Galera is not supported or not enabled')
                return False
            else:
                LOG.error(error_message)
                raise

    @contextmanager
    def get_connection(self):
        """
        Connect to MySQL host and yield a connection.

        :return: MySQL connection
        """
        connection = None
        try:
            connection = pymysql.connect(
                host=self._connect_info.hostname,
                read_default_file=self._connect_info.defaults_file,
                connect_timeout=self._connect_info.connect_timeout,
                cursorclass=self._connect_info.cursor
            )

            yield connection

        finally:
            if connection:
                connection.close()
