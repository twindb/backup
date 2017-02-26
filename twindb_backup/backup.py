# -*- coding: utf-8 -*-
"""Module that parses config file, builds a modifiers chain and fires
backup jobs.
"""
import ConfigParser
import base64
import errno
import fcntl
import os
import signal

from contextlib import contextmanager
from resource import getrlimit, RLIMIT_NOFILE, setrlimit
from twindb_backup import (
    LOG, get_directories_to_backup, get_timeout, LOCK_FILE,
    TwinDBBackupError)
from twindb_backup.configuration import get_destination
from twindb_backup.modifiers.base import ModifierException
from twindb_backup.modifiers.gpg import Gpg
from twindb_backup.modifiers.gzip import Gzip
from twindb_backup.modifiers.keeplocal import KeepLocal
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mysql_source import MySQLSource, MySQLConnectInfo


def backup_files(run_type, config):
    """Backup local directories

    :param run_type: Run type
    :type run_type: str
    :param config: Configuration
    :type config: ConfigParser.ConfigParser
    """
    for directory in get_directories_to_backup(config):
        LOG.debug('copying %s', directory)
        src = FileSource(directory, run_type)
        dst = get_destination(config)

        stream = src.get_stream()

        # Gzip modifier
        stream = Gzip(stream).get_stream()
        src.suffix += '.gz'

        # KeepLocal modifier
        try:
            keep_local_path = config.get('destination', 'keep_local_path')
            # src.suffix = 'tar.gz.aaa'
            dst_name = src.get_name()
            kl_modifier = KeepLocal(stream,
                                    os.path.join(keep_local_path, dst_name))
            stream = kl_modifier.get_stream()
        except ConfigParser.NoOptionError:
            pass

        # GPG modifier
        try:
            keyring = config.get('gpg', 'keyring')
            recipient = config.get('gpg', 'recipient')
            gpg = Gpg(stream, recipient, keyring)
            stream = gpg.get_stream()
            src.suffix += '.gpg'
        except ConfigParser.NoOptionError:
            pass
        except ModifierException as err:
            LOG.warning(err)
            LOG.warning('Will skip encryption')

        dst.save(stream, src.get_name())

        src.apply_retention_policy(dst, config, run_type)


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

    full_backup = 'daily'
    try:
        full_backup = config.get('mysql', 'full_backup')
    except ConfigParser.NoOptionError:
        pass

    src = MySQLSource(MySQLConnectInfo(config.get('mysql',
                                                  'mysql_defaults_file')),
                      run_type,
                      full_backup,
                      dst)

    callbacks = []
    stream = src.get_stream()

    # Gzip modifier
    stream = Gzip(stream).get_stream()
    src.suffix += '.gz'

    # KeepLocal modifier
    try:
        keep_local_path = config.get('destination', 'keep_local_path')
        kl_modifier = KeepLocal(stream,
                                os.path.join(keep_local_path,
                                             src.get_name()))
        stream = kl_modifier.get_stream()

        callbacks.append((kl_modifier, {
            'keep_local_path': keep_local_path,
            'dst': dst
        }))

    except ConfigParser.NoOptionError:
        LOG.debug('keep_local_path is not present in the config file')

    # GPG modifier
    try:
        stream = Gpg(stream, config.get('gpg', 'recipient'),
                     config.get('gpg', 'keyring')).get_stream()
        src.suffix += '.gpg'
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        pass
    except ModifierException as err:
        LOG.warning(err)
        LOG.warning('Will skip encryption')

    if dst.save(stream, src.get_name()) != 0:
        LOG.error('Failed to save backup copy %s', src.get_name())
        exit(1)

    status = dst.status()
    src_name = src.get_name()

    status[run_type][src_name] = {
        'binlog': src.binlog_coordinate[0],
        'position': src.binlog_coordinate[1],
        'lsn': src.lsn,
        'type': src.type
    }
    status[run_type][src_name]['config'] = []
    for path, content in src.get_my_cnf():
        status[run_type][src_name]['config'].append({
            path: base64.b64encode(content)
        })

    if src.incremental:
        status[run_type][src_name]['parent'] = src.parent

    if src.galera:
        status[run_type][src_name]['wsrep_provider_version'] = \
            src.wsrep_provider_version

    src.apply_retention_policy(dst, config, run_type, status)

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
        backup_files(run_type, config)
        backup_mysql(run_type, config)

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
    def timeout_handler(signum, frame):  # pylint: disable=unused-argument
        """Function to call on a timeout event"""
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
                raise err
            msg = 'Another instance of twindb-backup is running?'
            if run_type == 'hourly':
                LOG.debug(msg)
            else:
                LOG.error(msg)
