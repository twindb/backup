# -*- coding: utf-8 -*-
"""
Module with helper functions
"""
import errno
import os
import shutil
from contextlib import contextmanager
from subprocess import Popen, PIPE

from twindb_backup import LOG, INTERVALS, TwinDBBackupError


def mkdir_p(path):
    """
    Emulate mkdir -p

    :param path: Directory path.
    :type path: str
    """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def ensure_empty(path):
    """
    Check if a given directory is empty and exit if not.

    :param path: path to directory
    :type path: str
    """
    try:
        if os.listdir(path):
            msg = 'You asked to restore backup copy in directory "%s". ' \
                  'But it is not empty.' % path
            raise TwinDBBackupError(msg)

    except OSError as err:
        if err.errno == 2:  # OSError: [Errno 2] No such file or directory
            pass
        else:
            raise


def empty_dir(path):
    """Remove all files are directories in path

    :param path: Path to directory to be emptied.
    :type path: str
    """
    for the_file in os.listdir(path):
        file_path = os.path.join(path, the_file)
        if os.path.isfile(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


def get_hostname_from_backup_copy(backup_copy):
    """
    Backup copy includes hostname where the backup was taken from.
    The function extracts the hostname from the backup name.

    :param backup_copy: Backup copy name.
    :type backup_copy: str
    :return: Hostname where the backup was taken from.
    :rtype: str
    """
    chunks = backup_copy.split('/')
    for run_type in INTERVALS:
        if run_type in chunks:
            return chunks[chunks.index(run_type) - 1]
    return None


@contextmanager
def run_command(command, ok_non_zero=False):
    """
    Run shell command locally

    :param command: Command to run
    :type command: list
    :param ok_non_zero: Don't consider non-zero exit code as an error.
    :type ok_non_zero: bool
    :return: file object with stdout as generator to use with ``with``
    """
    try:
        LOG.debug('Running %s', " ".join(command))
        proc = Popen(command, stderr=PIPE, stdout=PIPE)

        yield proc.stdout

        _, cerr = proc.communicate()

        if proc.returncode and not ok_non_zero:
            LOG.error('Command %s exited with error code %d',
                      ' '.join(command),
                      proc.returncode)
            LOG.error(cerr)
            exit(1)
        else:
            LOG.debug('Exited with zero code')

    except OSError as err:
        LOG.error('Failed to run %s',
                  ' '.join(command))
        LOG.error(err)
        exit(1)


def split_host_port(host_port):
    """
    Splits a string of host and port separated by a semicolon.

    :param host_port: host or host:port. Allowed values are like
        10.20.31.1:3306 or just 10.20.31.1
    :return: a tuple with host and port. If only address is specified it'll
        return (address, None). If host_port is None it will
        return (None, None)
    :rtype: tuple
    """
    try:
        host = host_port.split(':')[0]
        if not host:
            host = None
    except AttributeError:
        host = None
    try:
        port = int(host_port.split(':')[1])
    except (IndexError, AttributeError, ValueError):
        port = None
    return host, port
