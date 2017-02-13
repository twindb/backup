# -*- coding: utf-8 -*-
"""
Module with helper functions
"""
import ConfigParser
import errno
import os
import socket

from twindb_backup import LOG, INTERVALS
from twindb_backup.destination.s3 import S3
from twindb_backup.destination.ssh import Ssh


def get_destination(config, hostname=socket.gethostname()):
    """
    Read config and return instance of Destination class.

    :param config: Tool configuration.
    :type config: ConfigParser.ConfigParser
    :param hostname: Local hostname.
    :type hostname: str
    :return: Instance of destination class.
    :rtype: BaseDestination
    """
    destination = None
    try:
        destination = config.get('destination', 'backup_destination')
        LOG.debug('Destination in the config %s', destination)
        destination = destination.strip('"\'')
    except ConfigParser.NoOptionError:
        LOG.critical("Backup destination must be specified "
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
                   key=ssh_key, hostname=hostname)

    elif destination == "s3":
        bucket = config.get('s3', 'BUCKET').strip('"\'')
        access_key_id = config.get('s3', 'AWS_ACCESS_KEY_ID').strip('"\'')
        secret_access_key = config.get('s3',
                                       'AWS_SECRET_ACCESS_KEY').strip('"\'')
        default_region = config.get('s3', 'AWS_DEFAULT_REGION').strip('"\'')
        return S3(bucket, access_key_id, secret_access_key,
                  default_region=default_region, hostname=hostname)

    else:
        LOG.critical('Destination %s is not supported', destination)
        exit(-1)


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
