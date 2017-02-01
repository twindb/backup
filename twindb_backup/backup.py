import ConfigParser
import base64
import errno
import fcntl
import signal

from contextlib import contextmanager
from resource import getrlimit, RLIMIT_NOFILE, setrlimit
from twindb_backup import (
    log, get_directories_to_backup, get_timeout, LOCK_FILE
)
from twindb_backup.destination.local import Local
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mysql_source import MySQLSource
from twindb_backup.util import get_destination


def backup_files(run_type, config):
    for d in get_directories_to_backup(config):
        log.debug('copying %s', d)
        src = FileSource(d, run_type)
        dst = get_destination(config)

        with src.get_stream() as stream:
            try:
                keep_local = config.get('destination', 'keep_local_path')
            except ConfigParser.NoOptionError:
                keep_local = None

            dst.save(stream, src.get_name(), keep_local=keep_local)
        src.apply_retention_policy(dst, config, run_type)


def backup_mysql(run_type, config):
    try:
        if config.getboolean('source', 'backup_mysql'):
            mysql_defaults_file = config.get('mysql', 'mysql_defaults_file')
            dst = get_destination(config)

            src = MySQLSource(mysql_defaults_file, run_type, config, dst)
            dst_name = src.get_name()

            try:
                keep_local = config.get('destination', 'keep_local_path')
            except ConfigParser.NoOptionError:
                keep_local = None

            with src.get_stream() as stream:
                if not dst.save(stream, dst_name, keep_local=keep_local):
                    log.error('Failed to save backup copy %s', dst_name)

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

            if keep_local:
                dst = Local(keep_local)
                dst.status(status)
    except ConfigParser.NoOptionError:
        log.debug('Not backing up MySQL')


def set_open_files_limit():
    max_files = getrlimit(RLIMIT_NOFILE)[0]
    while True:
        try:
            setrlimit(RLIMIT_NOFILE, (max_files, max_files))
            max_files += 1
        except ValueError:
            break
    log.debug('Setting max files limit to %d' % max_files)


def backup_everything(run_type, config):
    """
    Run backup job

    :param run_type: hourly, daily, etc
    :param config: ConfigParser instance
    """
    set_open_files_limit()

    try:
        backup_files(run_type, config)
        backup_mysql(run_type, config)

    except ConfigParser.NoSectionError as err:
        log.error(err)
        exit(1)


@contextmanager
def timeout(seconds):
    def timeout_handler(signum, frame):
        pass

    original_handler = signal.signal(signal.SIGALRM, timeout_handler)

    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


def run_backup_job(cfg, run_type, lock_file=LOCK_FILE):
    with timeout(get_timeout(run_type)):
        try:
            fd = open(lock_file, 'w')
            fcntl.flock(fd, fcntl.LOCK_EX)
            log.debug(run_type)
            if cfg.getboolean('intervals', "run_%s" % run_type):
                backup_everything(run_type, cfg)
            else:
                log.debug('Not running because run_%s is no', run_type)
        except IOError as err:
            if err.errno != errno.EINTR:
                raise err
            msg = 'Another instance of twindb-backup is running?'
            if run_type == 'hourly':
                log.debug(msg)
            else:
                log.error(msg)
