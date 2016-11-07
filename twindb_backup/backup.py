# -*- coding: utf-8 -*-
import ConfigParser
from contextlib import contextmanager
from resource import getrlimit, RLIMIT_NOFILE, setrlimit

import MySQLdb
import time
import signal
import fcntl
import errno

from twindb_backup import log, get_directories_to_backup, get_timeout, \
    LOCK_FILE
from twindb_backup.destination.s3 import S3
from twindb_backup.destination.ssh import Ssh
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mysql_source import MySQLSource


def get_destination(config):
    destination = None
    try:
        destination = config.get('destination', 'backup_destination')
        log.debug('Destination in the config %s', destination)
        destination = destination.strip('"\'')
    except ConfigParser.NoOptionError:
        log.critical("Backup destination must be specified "
                     "in the config file")
        exit(-1)

    if destination == "ssh":
        host = config.get('ssh', 'backup_host')
        try:
            port = config.get('ssh', 'port')
        except ConfigParser.NoOptionError:
            port = 22
        try:
            ssh_key = config.get('ssh', 'ssh_key')
        except ConfigParser.NoOptionError:
            ssh_key = '/root/.ssh/id_rsa'
        user = config.get('ssh', 'ssh_user')
        remote_path = config.get('ssh', 'backup_dir')
        return Ssh(host=host, port=port, user=user, remote_path=remote_path,
                   key=ssh_key)

    elif destination == "s3":
        bucket = config.get('s3', 'BUCKET').strip('"\'')
        access_key_id = config.get('s3', 'AWS_ACCESS_KEY_ID').strip('"\'')
        secret_access_key = config.get('s3',
                                       'AWS_SECRET_ACCESS_KEY').strip('"\'')
        default_region = config.get('s3', 'AWS_DEFAULT_REGION').strip('"\'')
        return S3(bucket, access_key_id, secret_access_key,
                  default_region=default_region)

    else:
        log.critical('Destination %s is not supported' % destination)
        exit(-1)


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


def enable_wsrep_desync(mysql_defaults_file):
    """
    Try to enable wsrep_desync

    :param mysql_defaults_file:
    :return: True if wsrep_desync was enabled. False if not supported
    """
    try:
        db = MySQLdb.connect(host='127.0.0.1',
                             read_default_file=mysql_defaults_file)
        c = db.cursor()
        c.execute("set global wsrep_desync=ON")
        return True
    except MySQLdb.Error as err:
        log.debug(err)
        return False


def execute_show_wsrep_local_recv_queue(cursor):
    cursor.execute("show global status like 'wsrep_local_recv_queue'")


def execute_wsrep_desync_off(cursor):
    cursor.execute("set global wsrep_desync=OFF")


def execute_wsrep_desync_off_on_timeout(cursor):
    cursor.execute("set global wsrep_desync=OFF")


def disable_wsrep_desync(mysql_defaults_file):
    """
    Wait till wsrep_local_recv_queue is zero
    and disable wsrep_local_recv_queue then

    :param mysql_defaults_file:
    """
    max_time = time.time() + 900
    try:
        db = MySQLdb.connect(host='127.0.0.1',
                             read_default_file=mysql_defaults_file)
        c = db.cursor()

        while time.time() < max_time:
            execute_show_wsrep_local_recv_queue(c)
            c.execute("show global status like 'wsrep_local_recv_queue'")

            if int(c.fetchone()[1]) == 0:
                log.debug('wsrep_local_recv_queue is zero')
                execute_wsrep_desync_off(c)
                return

            time.sleep(1)

        log.debug('Timeout expired. Disabling wsrep_desync')

        execute_wsrep_desync_off_on_timeout(c)

    except MySQLdb.Error as err:
        log.error(err)
    except TypeError:
        log.error('Galera not supported')


def backup_mysql(run_type, config):
    try:
        if config.getboolean('source', 'backup_mysql'):
            mysql_defaults_file = config.get('mysql', 'mysql_defaults_file')
            desync_enabled = enable_wsrep_desync(mysql_defaults_file)
            src = MySQLSource(mysql_defaults_file, run_type)
            dst = get_destination(config)
            dst_name = src.get_name()

            with src.get_stream() as stream:
                try:
                    keep_local = config.get('destination', 'keep_local_path')
                except ConfigParser.NoOptionError:
                    keep_local = None

                if dst.save(stream, dst_name, keep_local=keep_local):
                    log.error('Failed to save backup copy %s', dst_name)

            if desync_enabled:
                disable_wsrep_desync(mysql_defaults_file)

            src.apply_retention_policy(dst, config, run_type)
    except ConfigParser.NoOptionError:
        log.debug('Not backing up MySQL')


def set_open_fileslimit():
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
    RUn backup job

    :param run_type: hourly, daily, etc
    :param config: ConfigParser instance
    """
    set_open_fileslimit()

    try:
        backup_files(run_type, config)
        backup_mysql(run_type, config)

    except ConfigParser.NoSectionError as err:
        log.error('Config file must define section "source": %s', err)
        exit(-1)


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
            log.error('Another instance of twindb-backup is running?')
