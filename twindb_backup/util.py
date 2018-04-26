# -*- coding: utf-8 -*-
"""
Module with helper functions
"""
import errno
import multiprocessing
import os
import shutil
from contextlib import contextmanager
from subprocess import Popen, PIPE

import psutil

from twindb_backup import LOG, INTERVALS, TwinDBBackupError


def mkdir_p(path, mode=0777):
    """
    Emulate mkdir -p.
    Create a directory named path with numeric mode mode.
    The default mode is 0777 (octal)

    :param path: Directory path.
    :type path: str
    :param mode: Directory permissions. The default mode is 0777 (octal)
    :type mode: int
    """
    try:
        os.makedirs(path, mode=mode)
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


def kill_children():
    """
    Kill child process
    """
    for proc in multiprocessing.active_children():
        LOG.info('Terminating %r [%d] ...', proc, proc.pid)
        proc.terminate()
    parent = psutil.Process(os.getpid())
    for child in parent.children(recursive=True):
        LOG.info('Terminating process %r', child)
        child.kill()


def my_cnfs(common_paths=None):
    """
    Start reading a root my.cnf file given in common paths and parse included
    files.

    :param common_paths: list of my.cnf files to start parsing from.
    :type common_paths: list
    :return: list of all included my.cnf files
    :rtype: list
    """
    result = []
    for my_cnf in common_paths:
        if os.path.exists(my_cnf):
            result.append(my_cnf)
            with open(my_cnf) as fp_my_cnf:
                for line in fp_my_cnf.read().splitlines():
                    if '!includedir' in line:
                        path = line.split()[1]
                        c_paths = []
                        for included_file in os.listdir(path):
                            if included_file.endswith('.cnf'):
                                c_paths.append("%s%s" % (path, included_file))
                        result.extend(
                            my_cnfs(common_paths=c_paths)
                        )
                    elif '!include' in line:

                        include_file = line.split()[1]
                        result.extend(
                            my_cnfs(common_paths=[include_file])
                        )
    return result


def normalize_b64_data(coding):
    """
    Normalize base64 key. See http://bit.ly/2vxIAnC for details.

    :param coding: Encoded data
    :return: Normalized encoded data
    """
    missing_padding = len(coding) % 4
    if missing_padding != 0:
        coding += b'=' * (4 - missing_padding)
    return coding
