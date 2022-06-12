# -*- coding: utf-8 -*-
"""Module that parses config file, builds a modifiers chain and fires
backup jobs.
"""
import configparser
import errno
import fcntl
import signal
import time
import traceback
from contextlib import contextmanager
from os import path as osp
from resource import RLIMIT_NOFILE, getrlimit, setrlimit

from pymysql import InternalError

from twindb_backup import (
    LOCK_FILE,
    LOG,
    MY_CNF_COMMON_PATHS,
    get_timeout,
    save_measures,
)
from twindb_backup.copy.binlog_copy import BinlogCopy
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.destination.exceptions import DestinationError
from twindb_backup.exceptions import LockWaitTimeoutError, OperationError
from twindb_backup.export import export_info
from twindb_backup.exporter.base_exporter import (
    ExportCategory,
    ExportMeasureType,
)
from twindb_backup.modifiers.gpg import Gpg
from twindb_backup.modifiers.keeplocal import KeepLocal
from twindb_backup.source.binlog_source import BinlogParser, BinlogSource
from twindb_backup.source.exceptions import SourceError
from twindb_backup.source.file_source import FileSource
from twindb_backup.source.mariadb_source import MariaDBSource
from twindb_backup.source.mysql_source import (
    MySQLClient,
    MySQLConnectInfo,
    MySQLFlavor,
    MySQLSource,
)
from twindb_backup.ssh.exceptions import SshClientException
from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.mysql_status import MySQLStatus
from twindb_backup.util import my_cnfs

MYSQL_SRC_MAP = {
    MySQLFlavor.MARIADB: MariaDBSource,
    MySQLFlavor.ORACLE: MySQLSource,
    MySQLFlavor.PERCONA: MySQLSource,
}


def _backup_stream(config, src, dst, callbacks=None):
    """

    :param config: Tool config
    :type config: TwinDBBackupConfig
    :param src:
    :param dst:
    :param callbacks:
    :return:
    """
    stream = src.get_stream()

    # Compression modifier
    cmp_modifier = config.compression.get_modifier(stream)
    stream = cmp_modifier.get_stream()
    src.suffix += cmp_modifier.suffix

    # KeepLocal modifier
    if config.keep_local_path:
        keep_local_path = config.keep_local_path
        kl_modifier = KeepLocal(
            stream, osp.join(keep_local_path, src.get_name())
        )
        stream = kl_modifier.get_stream()
        if callbacks is not None:
            callbacks.append(
                (kl_modifier, {"keep_local_path": keep_local_path, "dst": dst})
            )
    else:
        LOG.debug("keep_local_path is not present in the config file")
    # GPG modifier
    if config.gpg:
        gpg_modifier = Gpg(stream, config.gpg.recipient, config.gpg.keyring)

        stream = gpg_modifier.get_stream()
        src.suffix += ".gpg"
    dst.save(stream, src.get_name())


def backup_files(run_type, config):
    """Backup local directories

    :param run_type: Run type
    :type run_type: str
    :param config: Configuration
    :type config: TwinDBBackupConfig
    """
    backup_start = time.time()
    try:
        for directory in config.backup_dirs:
            LOG.debug("copying %s", directory)
            src = FileSource(directory, run_type)
            dst = config.destination()
            _backup_stream(config, src, dst)
            src.apply_retention_policy(dst, config, run_type)
    except (DestinationError, SourceError, SshClientException) as err:
        raise OperationError(err)
    export_info(
        config,
        data=time.time() - backup_start,
        category=ExportCategory.files,
        measure_type=ExportMeasureType.backup,
    )


def backup_mysql(run_type, config):
    """Take backup of local MySQL instance

    :param run_type: Run type
    :type run_type: str
    :param config: Tool configuration
    :type config: TwinDBBackupConfig
    """
    if config.backup_mysql is False:
        LOG.debug("Not backing up MySQL")
        return

    dst = config.destination()
    backup_start = time.time()
    status = MySQLStatus(dst=dst)

    kwargs = {
        "backup_type": status.next_backup_type(
            config.mysql.full_backup, run_type
        ),
        "dst": dst,
        "xtrabackup_binary": config.mysql.xtrabackup_binary,
    }
    parent = status.candidate_parent(run_type)

    if kwargs["backup_type"] == "incremental":
        kwargs["parent_lsn"] = parent.lsn

    LOG.debug("Creating source %r", kwargs)
    mysql_client = MySQLClient(config.mysql.defaults_file)
    src = MYSQL_SRC_MAP[mysql_client.server_vendor](
        MySQLConnectInfo(config.mysql.defaults_file), run_type, **kwargs
    )

    callbacks = []
    try:
        _backup_stream(config, src, dst, callbacks=callbacks)
    except (DestinationError, SourceError, SshClientException) as err:
        raise OperationError(err)
    LOG.debug("Backup copy name: %s", src.get_name())

    kwargs = {
        "type": src.type,
        "binlog": src.binlog_coordinate[0],
        "position": src.binlog_coordinate[1],
        "lsn": src.lsn,
        "backup_started": backup_start,
        "backup_finished": time.time(),
        "config_files": my_cnfs(MY_CNF_COMMON_PATHS),
        "server_vendor": src.server_vendor,
    }
    if src.incremental:
        kwargs["parent"] = parent.key

    backup_copy = MySQLCopy(src.host, run_type, src.basename, **kwargs)
    status.add(backup_copy)

    status = src.apply_retention_policy(dst, config, run_type, status)
    LOG.debug("status after apply_retention_policy():\n%s", status)

    backup_duration = backup_copy.duration
    export_info(
        config,
        data=backup_duration,
        category=ExportCategory.mysql,
        measure_type=ExportMeasureType.backup,
    )

    status.save(dst)

    LOG.debug("Callbacks are %r", callbacks)
    for callback in callbacks:
        callback[0].callback(**callback[1])


def backup_binlogs(run_type, config):  # pylint: disable=too-many-locals
    """Copy MySQL binlog files to the backup destination.

    :param run_type: Run type
    :type run_type: str
    :param config: Tool configuration
    :type config: TwinDBBackupConfig
    """
    if config.mysql is None:
        LOG.debug("No MySQL config, not copying binlogs")
        return

    dst = config.destination()
    status = BinlogStatus(dst=dst)
    mysql_client = MySQLClient(defaults_file=config.mysql.defaults_file)
    log_bin_basename = mysql_client.variable("log_bin_basename")
    if log_bin_basename is None:
        return
    binlog_dir = osp.dirname(log_bin_basename)

    # last_copy = status.latest_backup
    LOG.debug("Latest copied binlog %s", status.latest_backup)
    with mysql_client.cursor() as cur:
        cur.execute("FLUSH BINARY LOGS")
        backup_set = binlogs_to_backup(
            cur,
            last_binlog=status.latest_backup.name
            if status.latest_backup
            else None,
        )

    for binlog_name in backup_set:
        src = BinlogSource(run_type, mysql_client, binlog_name)
        binlog_copy = BinlogCopy(
            src.host,
            binlog_name,
            BinlogParser(osp.join(binlog_dir, binlog_name)).created_at,
        )
        _backup_stream(config, src, dst)
        status.add(binlog_copy)

    try:
        expire_log_days = config.mysql.expire_log_days
    except (configparser.NoSectionError, configparser.NoOptionError):
        expire_log_days = 7

    for copy in status:
        now = int(time.time())
        LOG.debug("Reviewing copy %s. Now: %d", copy, now)

        if copy.created_at < now - expire_log_days * 24 * 3600:
            LOG.debug(
                "Deleting copy that was taken %d seconds ago",
                now - copy.created_at,
            )
            dst.delete(copy.key + ".gz")
            status.remove(copy.key)

    status.save(dst)


def binlogs_to_backup(cursor, last_binlog=None):
    """
    Finds list of binlogs to copy. It will return the binlogs
    from the last to the current one (excluding it).
    If binlog are not enabled in the server the function will return
    empty list.

    :param cursor: MySQL cursor
    :param last_binlog: Name of the last copied binlog.
    :return: list of binlogs to backup.
    :rtype: list
    """
    binlogs = []
    try:
        cursor.execute("SHOW BINARY LOGS")
        for row in cursor.fetchall():
            binlog = row["Log_name"]
            if not last_binlog or binlog > last_binlog:
                binlogs.append(binlog)

        return binlogs[:-1]
    except InternalError as err:
        if err.args and err.args[0] == 1381:  # ER_NO_BINARY_LOGGING
            return binlogs
        else:
            raise OperationError(err)


def set_open_files_limit():
    """Detect maximum supported number of open file and set it"""
    max_files = getrlimit(RLIMIT_NOFILE)[0]
    while True:
        try:
            setrlimit(RLIMIT_NOFILE, (max_files, max_files))
            max_files += 1
        except ValueError:
            break
    LOG.debug("Setting max files limit to %d", max_files)


def backup_everything(run_type, twindb_config, binlogs_only=False):
    """
    Run backup job

    :param run_type: hourly, daily, etc
    :type run_type: str
    :param twindb_config: ConfigParser instance
    :type twindb_config: TwinDBBackupConfig
    :param binlogs_only: If True copy only MySQL binary logs.
    :type binlogs_only: bool
    """
    set_open_files_limit()

    try:
        if not binlogs_only:
            backup_start = time.time()
            backup_files(run_type, twindb_config)
            backup_mysql(run_type, twindb_config)
            backup_binlogs(run_type, twindb_config)
            end = time.time()
            save_measures(backup_start, end)
        else:
            backup_binlogs(run_type, twindb_config)
    except configparser.NoSectionError as err:
        LOG.debug(traceback.format_exc())
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


def run_backup_job(
    twindb_config, run_type, lock_file=LOCK_FILE, binlogs_only=False
):
    """
    Grab a lock waiting up to allowed timeout and start backup jobs

    :param twindb_config: Tool configuration
    :type twindb_config: TwinDBBackupConfig
    :param run_type: Run type
    :type run_type: str
    :param lock_file: File used as a lock
    :type lock_file: str
    :param binlogs_only: If True copy only binlogs.
    :type binlogs_only: bool
    """
    with timeout(get_timeout(run_type)):
        try:
            file_desriptor = open(lock_file, "w")
            fcntl.flock(file_desriptor, fcntl.LOCK_EX)
            LOG.debug(run_type)
            if getattr(twindb_config.run_intervals, run_type):
                backup_everything(
                    run_type, twindb_config, binlogs_only=binlogs_only
                )
            else:
                LOG.debug("Not running because run_%s is no", run_type)
        except IOError as err:
            if err.errno != errno.EINTR:
                LOG.debug(traceback.format_exc())
                raise LockWaitTimeoutError(err)
            msg = "Another instance of twindb-backup is running?"
            if run_type == "hourly":
                LOG.debug(msg)
            else:
                LOG.error(msg)
