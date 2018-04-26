# -*- coding: utf-8 -*-
"""
Module to process configuration file.
"""
import ConfigParser
import socket

from twindb_backup import LOG
from twindb_backup.destination.s3 import S3, AWSAuthOptions
from twindb_backup.destination.ssh import Ssh, SshConnectInfo
from twindb_backup.exporter.datadog_exporter import DataDogExporter


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
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
        LOG.critical("Backup destination must be specified "
                     "in the config file")
        exit(-1)

    if destination == "ssh":
        host = config.get('ssh', 'backup_host')
        try:
            port = int(config.get('ssh', 'port'))
        except ConfigParser.NoOptionError:
            port = 22
        try:
            ssh_key = config.get('ssh', 'ssh_key')
        except ConfigParser.NoOptionError:
            ssh_key = '/root/.ssh/id_rsa'
            LOG.debug('ssh_key is not defined in config. '
                      'Will use default %s', ssh_key)
        user = config.get('ssh', 'ssh_user')
        remote_path = config.get('ssh', 'backup_dir')
        return Ssh(
            remote_path,
            SshConnectInfo(
                host=host,
                port=port,
                user=user,
                key=ssh_key),
            hostname=hostname)

    elif destination == "s3":
        bucket = config.get('s3', 'BUCKET').strip('"\'')
        access_key_id = config.get('s3', 'AWS_ACCESS_KEY_ID').strip('"\'')
        secret_access_key = config.get('s3',
                                       'AWS_SECRET_ACCESS_KEY').strip('"\'')
        default_region = config.get('s3', 'AWS_DEFAULT_REGION').strip('"\'')

        return S3(bucket, AWSAuthOptions(access_key_id,
                                         secret_access_key,
                                         default_region=default_region),
                  hostname=hostname)

    else:
        LOG.critical('Destination %s is not supported', destination)
        exit(-1)


def get_export_transport(config):
    """
    Read config and return export transport instance

    :param config: Config file
    :return: Instance of export transport, if it set
    :raise: NotImplementedError, if transport isn't implemented
    """
    try:
        transport = config.get("export", "transport")
        if transport == "datadog":
            app_key = config.get("export", "app_key")
            api_key = config.get("export", "api_key")
            return DataDogExporter(app_key, api_key)
        raise NotImplementedError
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        return None
