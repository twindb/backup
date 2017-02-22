# -*- coding: utf-8 -*-
"""
Module with helper functions
"""
import errno
import os
from contextlib import contextmanager
from subprocess import Popen, PIPE

from twindb_backup import LOG, INTERVALS


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
