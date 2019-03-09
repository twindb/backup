# -*- coding: utf-8 -*-
"""TwinDB Backup module.

 The module is a core of twindb-backup tool. It includes backup and restore
 functionality. The module takes a backup from something defined in a source
 class and saves the backup copy in something defined in a destination class.

 The source class inherits from BaseSource() from
 twindb_backup.source.base_source.py. The source class must define get_stream()
 method that yields a file object that is used for next classes.
 Typical classes are FileSource() to backup files and directories,
 MySQLSource() to backup MySQL.

 The destination class inherits from BaseDestination(). This is where you
 store backups. The destination class must define save() method that
 takes an input stream and saves it somewhere. Examples of
 the destination class are S3(), Ssh().

 There are modifier classes. The modifier class sits in the middle between
 the source and the destination and does something with a stream before
 the stream is saved. The modifier class may save a local copy (KeepLocal())
 or encrypt the stream or else. The modifier class inherits Modifier()

 The backup process may be depicted as a chain of modifiers with the source
 in the head and the destination in the tail.

::

 +--------+    +------------+    +------------+    +-------------+
 | source | -- | modifier 1 | -- | modifier 2 | -- | destination |
 +--------+    +------------+    +------------+    +-------------+

 """
import ConfigParser
import glob
import json
import logging
import os

import sys

__author__ = 'TwinDB Development Team'
__email__ = 'dev@twindb.com'
__version__ = '2.16.1'
STATUS_FORMAT_VERSION = 1
LOCK_FILE = '/var/run/twindb-backup.lock'
LOG_FILE = '/var/log/twindb-backup-measures.log'
INTERVALS = ['hourly', 'daily', 'weekly', 'monthly', 'yearly']
MEDIA_TYPES = ['files', 'mysql', 'binlog']
XTRABACKUP_BINARY = '/opt/twindb-backup/embedded/bin/xtrabackup'
XBSTREAM_BINARY = '/opt/twindb-backup/embedded/bin/xbstream'
MY_CNF_COMMON_PATHS = [
    '/etc/my.cnf',
    '/etc/mysql/my.cnf'
]

LOG = logging.getLogger(__name__)


class TwinDBBackupError(Exception):
    """Class for script errors"""


class LessThanFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """Filters out log messages of a lower level."""

    def __init__(self, exclusive_maximum, name=""):
        super(LessThanFilter, self).__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        # non-zero return means we log this message
        return 1 if record.levelno < self.max_level else 0


def setup_logging(logger, debug=False):  # pragma: no cover
    """Configures logging for the module"""

    fmt_str = "%(asctime)s: %(levelname)s:" \
              " %(module)s.%(funcName)s():%(lineno)d: %(message)s"

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.addFilter(LessThanFilter(logging.WARNING))
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(fmt_str))

    # Log errors and warnings to stderr
    console_handler_err = logging.StreamHandler(stream=sys.stderr)
    console_handler_err.setLevel(logging.WARNING)
    console_handler_err.setFormatter(logging.Formatter(fmt_str))

    # Log debug to stderr
    console_handler_debug = logging.StreamHandler(stream=sys.stderr)
    console_handler_debug.addFilter(LessThanFilter(logging.INFO))
    console_handler_debug.setLevel(logging.DEBUG)
    console_handler_debug.setFormatter(logging.Formatter(fmt_str))

    logger.handlers = []
    logger.addHandler(console_handler)
    logger.addHandler(console_handler_err)

    if debug:
        logger.addHandler(console_handler_debug)
        logger.debug_enabled = True

    logger.setLevel(logging.DEBUG)


def get_files_to_delete(all_files, keep_copies):
    """If you give it a list of files and number of how many
    you'd like to keep the function will return files that need
    to be deleted

    :param all_files: list of strings
    :type all_files: list
    :param keep_copies: number of copied to keep
    :type keep_copies: int
    :return: list of strings (files) to delete
    :rtype: list
    """
    LOG.debug('Retain %d files', keep_copies)
    if keep_copies == 0:
        return all_files
    else:
        return all_files[:-keep_copies]


def delete_local_files(dir_backups, keep_copies):
    """Deletes local backup copies based on given retention number.

    :param dir_backups: directory with backup copies
    :type dir_backups: str
    :param keep_copies: how many to keep
    :type keep_copies: int
    :return: None

    """
    local_files = sorted(glob.glob(dir_backups))
    LOG.debug('Local copies: %r', local_files)

    for local_file in get_files_to_delete(local_files, keep_copies):
        LOG.debug('Deleting: %s', local_file)
        os.unlink(local_file)


def get_timeout(run_type):
    """Get timeout for a each run type - daily, hourly etc

    :param run_type: Run type
    :type run_type: str
    :return: Number of seconds the tool allowed to wait until other instances
      finish
    :rtype: int
    """
    timeouts = {
        'hourly': 3600 / 2,
        'daily': 24 * 3600 / 2,
        'weekly': 7 * 24 * 3600 / 2,
        'monthly': 30 * 24 * 3600 / 2,
        'yearly': 365 * 24 * 3600 / 2
    }
    return timeouts[run_type]


def save_measures(start_time, end_time, log_path=LOG_FILE):
    """Save backup measures to log file"""
    data = {
        'start': start_time,
        'finish': end_time,
        'duration': end_time - start_time
    }
    try:
        with open(log_path, 'r') as data_fp:
            log = json.load(data_fp)
            log['measures'].append(data)
            if len(log['measures']) > 100:
                del log['measures'][0]
    except (IOError, ValueError):
        log = {
            'measures': [data]
        }

    with open(log_path, 'w') as file_pt:
        json.dump(log, file_pt)
