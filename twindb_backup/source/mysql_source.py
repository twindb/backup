# -*- coding: utf-8 -*-
"""
Module defines MySQL source class for backing up local MySQL.
"""
from __future__ import print_function
import os
from os import path as osp
import tempfile
import time

from contextlib import contextmanager
from subprocess import Popen, PIPE
import sys

import pymysql
from pymysql import OperationalError

from twindb_backup import LOG, get_files_to_delete, INTERVALS, \
    XTRABACKUP_BINARY
from twindb_backup.source.base_source import BaseSource
from twindb_backup.source.exceptions import MySQLSourceError
from twindb_backup.status.exceptions import StatusKeyNotFound


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


class MySQLMasterInfo(object):  # pylint: disable=too-few-public-methods
    """MySQL master details """
    def __init__(self, host, port,  # pylint: disable=too-many-arguments
                 user, password, binlog, binlog_pos):
        self.host = host
        self.user = user
        self.password = password
        self.binlog = binlog
        self.binlog_position = binlog_pos
        self.port = 3306 if port is None else port


class MySQLClient(object):
    """Class to send queries to MySQL"""
    def __init__(self, defaults_file,
                 connect_timeout=10,
                 hostname="127.0.0.1"):
        self.connect_timeout = connect_timeout
        self.defaults_file = defaults_file
        self.hostname = hostname

    @contextmanager
    def get_connection(self):
        """
        Connect to MySQL host and yield a connection.

        :return: MySQL connection
        :raise MySQLSourceError: if can't connect to server
        """
        connection = None
        try:
            connection = pymysql.connect(
                host=self.hostname,
                read_default_file=self.defaults_file,
                connect_timeout=self.connect_timeout,
                cursorclass=pymysql.cursors.DictCursor
            )

            yield connection
        except OperationalError:
            LOG.error(
                "Can't connect to MySQL server on %s",
                self.hostname)
            raise MySQLSourceError(
                "Can't connect to MySQL server on %s"
                % self.hostname)
        finally:
            if connection:
                connection.close()

    @contextmanager
    def cursor(self):
        """MySQL cursor for connection to local MySQL instance."""
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                yield cursor

    def variable(self, varname):
        """Read MySQL variable and return its value"""
        with self.cursor() as cursor:
            cursor.execute("SELECT @@%s AS varname" % varname)
            row = cursor.fetchone()
            return row['varname']


class MySQLSource(BaseSource):  # pylint: disable=too-many-instance-attributes
    """MySQLSource class"""

    def __init__(self, mysql_connect_info, run_type, backup_type, **kwargs):
        """
        MySQLSource constructor

        :param mysql_connect_info: MySQL connection details
        :type mysql_connect_info: MySQLConnectInfo
        :param run_type: daily, weekly, etc
        :param backup_type: full or incremental
        :type backup_type: str
        :param dst:
        """
        if run_type not in INTERVALS:
            raise MySQLSourceError('Incorrect run type %r' % run_type)
        self._parent_lsn = kwargs.get('parent_lsn', None)

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
        if backup_type in ['full', 'incremental']:
            self._type = backup_type
        else:
            raise MySQLSourceError('Unrecognized backup type %s' % backup_type)

        self._suffix = 'xbstream'
        self._media_type = 'mysql'
        self._file_name_prefix = 'mysql'
        self.dst = kwargs.get('dst', None)
        self._xtrabackup = kwargs.get('xtrabackup_binary', XTRABACKUP_BINARY)
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
        cmd = [
            self._xtrabackup,
            "--defaults-file=%s" % self._connect_info.defaults_file,
            "--stream=xbstream",
            "--host=127.0.0.1",
            "--backup"
        ]
        cmd += ["--target-dir", "."]
        if self.is_galera():
            cmd.append("--galera-info")
            cmd.append("--no-backup-locks")
        if self.incremental:
            cmd += [
                "--incremental-basedir",
                ".",
                "--incremental-lsn=%d" % self._parent_lsn
            ]
        # If this is a Galera node then additional step needs to be taken to
        # prevent the backups from locking up the cluster.
        wsrep_desynced = False
        LOG.debug('Running %s', ' '.join(cmd))
        stderr_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            if self.is_galera():
                wsrep_desynced = self.enable_wsrep_desync()

            LOG.debug('Running %s', ' '.join(cmd))
            proc_xtrabackup = Popen(cmd,
                                    stderr=stderr_file,
                                    stdout=PIPE)

            yield proc_xtrabackup.stdout

            proc_xtrabackup.communicate()
            if proc_xtrabackup.returncode:
                LOG.error('Failed to run xtrabackup. '
                          'Check error output in %s', stderr_file.name)
                try:
                    if LOG.debug_enabled:
                        with open(stderr_file.name) as xb_out:
                            for line in xb_out:
                                print(line, end='', file=sys.stderr)
                except AttributeError:
                    pass
                self.dst.delete(self.get_name())
                exit(1)
            else:
                LOG.debug('Successfully streamed xtrabackup output')
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
        LOG.error('Failed to run xtrabackup. '
                  'Check error output in %s', stderr_file.name)
        self.dst.delete(self.get_name())
        exit(1)

    def _update_backup_info(self, stderr_file):
        """Update backup_info from stderr"""

        LOG.debug('xtrabackup error log file %s',
                  stderr_file.name)
        self._backup_info.lsn = self._get_lsn(stderr_file.name)
        self._backup_info.binlog_coordinate = self.get_binlog_coordinates(
            stderr_file.name
        )
        os.unlink(stderr_file.name)

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
        :type config: TwinDBBackupConfig
        :param run_type: Run type.
        :type run_type: str
        :param status: Backups status.
        :type status: Status
        :return: Updated status.
        :rtype: Status
        """

        prefix = osp.join(
            dst.remote_path,
            self.get_prefix(),
            'mysql'
        )
        keep_copies = getattr(config.retention, run_type)

        backups_list = dst.list_files(
            prefix,
            files_only=True
        )
        LOG.debug('Remote copies: %r', backups_list)
        for backup_copy in get_files_to_delete(backups_list, keep_copies):
            LOG.debug('Deleting remote file %s', backup_copy)
            dst.delete(backup_copy)
            try:
                status.remove(dst.basename(backup_copy))
            except StatusKeyNotFound as err:
                LOG.warning(err)
                LOG.debug('Status: %r', status)

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
    def _get_lsn(err_log_path):
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
        return self.type == 'full'

    @property
    def incremental(self):
        """
        Check if the backup copy is an incremental copy.

        :return: True if it's an incremental copy.
        :rtype: bool
        """
        return not self.full

    @property
    def type(self):
        """Get backup copy type - full or incremental

        :return: 'full' or 'incremental'
        :rtype: str
        """
        return self._type
        # status = self.dst.status()
        # return status.get_backup_type(self.full_backup, self.run_type)

    @property
    def status(self):
        """Backup status on a destination

        :return: Backups status
        :rtype: dict
        """
        return self.dst.status()

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

    @property
    def wsrep_provider_version(self):
        """
        Parse Galera version from wsrep_provider_version.

        :return: Galera version
        :rtype: str
        """
        with self._cursor() as cursor:
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
            with self._cursor() as cursor:
                cursor.execute("SELECT @@wsrep_on as wsrep_on")
                row = cursor.fetchone()

                return (str(row['wsrep_on']).lower() == "1" or
                        str(row['wsrep_on']).lower() == 'on')
        except pymysql.InternalError as err:
            error_code = err.args[0]
            error_message = err.args[1]

            if error_code == 1193:
                LOG.debug('Galera is not supported or not enabled')
                return False
            else:
                LOG.error(error_message)
                raise

    @property
    def datadir(self):
        """Return datadir path on MySQL server"""
        with self._cursor() as cursor:
            cursor.execute("SELECT @@datadir AS datadir")
            row = cursor.fetchone()
            return row['datadir']

    @contextmanager
    def get_connection(self):
        """
        Connect to MySQL host and yield a connection.

        :return: MySQL connection
        :raise MySQLSourceError: if can't connect to server
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
        except OperationalError:
            LOG.error("Can't connect to MySQL server on %s",
                      self._connect_info.hostname)
            raise MySQLSourceError("Can't connect to MySQL server on %s" %
                                   self._connect_info.hostname)
        finally:
            if connection:
                connection.close()

    @contextmanager
    def _cursor(self):
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                yield cursor
