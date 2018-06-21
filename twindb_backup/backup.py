# -*- coding: utf-8 -*-
"""Module that parses config file, builds a modifiers chain and fires
backup jobs.
"""
import ConfigParser
import errno
import fcntl
import os
import signal
import time
from contextlib import contextmanager
from resource import getrlimit, RLIMIT_NOFILE, setrlimit
from twindb_backup import (
    LOG, get_directories_to_backup, get_timeout, LOCK_FILE,
    TwinDBBackupError, save_measures, XTRABACKUP_BINARY, MY_CNF_COMMON_PATHS)
from twindb_backup.configuration import get_destination
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.destination.exceptions import DestinationError
from twindb_backup.exceptions import OperationError, LockWaitTimeoutError
from twindb_backup.export import export_info
from twindb_backup.exporter.base_exporter import ExportCategory, \
    ExportMeasureType
from twindb_backup.modifiers.base import ModifierException
from twindb_backup.modifiers.gpg import Gpg
from twindb_backup.modifiers.gzip import Gzip
from twindb_backup.modifiers.keeplocal import KeepLocal
from twindb_backup.source.exceptions import SourceError
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo
from twindb_backup.ssh.exceptions import SshClientException
from twindb_backup.util import my_cnfs


def _backup_stream(config, src, dst, callbacks=None):
    stream = src.get_stream()
    # Gzip modifier
    stream = Gzip(stream).get_stream()
    src.suffix += '.gz'
    # KeepLocal modifier
    try:
        keep_local_path = config.get('destination', 'keep_local_path')
        kl_modifier = KeepLocal(
            stream,
            os.path.join(
                keep_local_path,
                src.get_name()
            )
        )
        stream = kl_modifier.get_stream()
        if callbacks is not None:
            callbacks.append((kl_modifier, {
                'keep_local_path': keep_local_path,
                'dst': dst
            }))
    except ConfigParser.NoOptionError:
        LOG.debug('keep_local_path is not present in the config file')
    # GPG modifier
    try:
        stream = Gpg(
            stream,
            config.get('gpg', 'recipient'),
            config.get('gpg', 'keyring')).get_stream()
        src.suffix += '.gpg'
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        pass
    except ModifierException as err:
        LOG.warning(err)
        LOG.warning('Will skip encryption')
    dst.save(stream, src.get_name())


def backup_files(run_type, config):
    """Backup local directories

    :param run_type: Run type
    :type run_type: str
    :param config: Configuration
    :type config: ConfigParser.ConfigParser
    """
    backup_start = time.time()
    try:
        for directory in get_directories_to_backup(config):
            LOG.debug('copying %s', directory)
            src = FileSource(directory, run_type)
            dst = get_destination(config)
            _backup_stream(config, src, dst)
            src.apply_retention_policy(dst, config, run_type)
    except (
            DestinationError,
            SourceError,
            SshClientException
    ) as err:
        raise OperationError(err)
    export_info(config, data=time.time() - backup_start,
                category=ExportCategory.files,
                measure_type=ExportMeasureType.backup)


def backup_mysql(run_type, config):
    """Take backup of local MySQL instance

    :param run_type: Run type
    :type run_type: str
    :param config: Tool configuration
    :type config: ConfigParser.ConfigParser
    :return: None
    """
    try:
        if not config.getboolean('source', 'backup_mysql'):
            raise TwinDBBackupError('MySQL backups are not enabled in config')

    except (ConfigParser.NoOptionError, TwinDBBackupError) as err:
        LOG.debug(err)
        LOG.debug('Not backing up MySQL')
        return

    dst = get_destination(config)

    try:
        full_backup = config.get('mysql', 'full_backup')
    except ConfigParser.NoOptionError:
        full_backup = 'daily'
    backup_start = time.time()
    try:
        xtrabackup_binary = config.get('mysql', 'xtrabackup_binary')
    except ConfigParser.NoOptionError:
        xtrabackup_binary = XTRABACKUP_BINARY

    status = dst.status()

    kwargs = {
        'backup_type': status.next_backup_type(full_backup, run_type),
        'dst': dst,
        'xtrabackup_binary': xtrabackup_binary
    }
    parent = status.eligble_parent(run_type)

    if kwargs['backup_type'] == 'incremental':
        kwargs['parent_lsn'] = parent.lsn

    LOG.debug('Creating source %r', kwargs)
    src = MySQLSource(
        MySQLConnectInfo(config.get('mysql', 'mysql_defaults_file')),
        run_type,
        **kwargs
    )

    callbacks = []
    try:
        _backup_stream(config, src, dst, callbacks=callbacks)
    except (
            DestinationError,
            SourceError,
            SshClientException
    ) as err:
        raise OperationError(err)
    LOG.debug('Backup copy name: %s', src.get_name())

    kwargs = {
        'type': src.type,
        'binlog': src.binlog_coordinate[0],
        'position': src.binlog_coordinate[1],
        'lsn': src.lsn,
        'backup_started': backup_start,
        'backup_finished': time.time(),
        'config_files': my_cnfs(MY_CNF_COMMON_PATHS)
    }
    if src.incremental:
        kwargs['parent'] = parent.key

    backup_copy = MySQLCopy(
        src.host,
        run_type,
        src.basename,
        **kwargs
    )
    status.add(backup_copy)

    status = src.apply_retention_policy(dst, config, run_type, status)
    LOG.debug('status after apply_retention_policy():\n%s', status)

    backup_duration = status.backup_duration(run_type, src.get_name())
    export_info(
        config,
        data=backup_duration,
        category=ExportCategory.mysql,
        measure_type=ExportMeasureType.backup
    )
    dst.status(status)

    LOG.debug('Callbacks are %r', callbacks)
    for callback in callbacks:
        callback[0].callback(**callback[1])


def set_open_files_limit():
    """Detect maximum supported number of open file and set it"""
    max_files = getrlimit(RLIMIT_NOFILE)[0]
    while True:
        try:
            setrlimit(RLIMIT_NOFILE, (max_files, max_files))
            max_files += 1
        except ValueError:
            break
    LOG.debug('Setting max files limit to %d', max_files)


def backup_everything(run_type, config):
    """
    Run backup job

    :param run_type: hourly, daily, etc
    :type run_type: str
    :param config: ConfigParser instance
    :type config: ConfigParser.ConfigParser
    """
    set_open_files_limit()

    try:
        backup_start = time.time()
        backup_files(run_type, config)
        backup_mysql(run_type, config)
        end = time.time()
        save_measures(backup_start, end)
    except ConfigParser.NoSectionError as err:
        LOG.error(err)
        exit(1)


@contextmanager
def timeout(seconds):
    """
    Implement timeout

    :param seconds: timeout in seconds
    :type seconds: int
    """

    def timeout_handler(signum, frame):
        """Function to call on a timeout event"""
        if signum or frame:
            pass

    original_handler = signal.signal(signal.SIGALRM, timeout_handler)

    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


def run_backup_job(cfg, run_type, lock_file=LOCK_FILE):
    """
    Grab a lock waiting up to allowed timeout and start backup jobs

    :param cfg: Tool configuration
    :type cfg: ConfigParser.ConfigParser
    :param run_type: Run type
    :type run_type: str
    :param lock_file: File used as a lock
    :type lock_file: str
    """
    with timeout(get_timeout(run_type)):
        try:
            file_desriptor = open(lock_file, 'w')
            fcntl.flock(file_desriptor, fcntl.LOCK_EX)
            LOG.debug(run_type)
            if cfg.getboolean('intervals', "run_%s" % run_type):
                backup_everything(run_type, cfg)
            else:
                LOG.debug('Not running because run_%s is no', run_type)
        except IOError as err:
            if err.errno != errno.EINTR:
                raise LockWaitTimeoutError(err)
            msg = 'Another instance of twindb-backup is running?'
            if run_type == 'hourly':
                LOG.debug(msg)
            else:
                LOG.error(msg)
