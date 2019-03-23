# -*- coding: utf-8 -*-
"""
Module to process configuration file.
"""
import socket
from ConfigParser import ConfigParser, NoOptionError, NoSectionError
from shlex import split

from twindb_backup import LOG, INTERVALS
from twindb_backup.configuration.destinations.s3 import S3Config
from twindb_backup.configuration.destinations.ssh import SSHConfig
from twindb_backup.configuration.exceptions import ConfigurationError
from twindb_backup.configuration.gpg import GPGConfig
from twindb_backup.configuration.mysql import MySQLConfig
from twindb_backup.configuration.retention import RetentionPolicy
from twindb_backup.configuration.run_intervals import RunIntervals
from twindb_backup.destination.s3 import S3
from twindb_backup.destination.ssh import Ssh
from twindb_backup.exporter.datadog_exporter import DataDogExporter

DEFAULT_CONFIG_FILE_PATH = '/etc/twindb/twindb-backup.cfg'


class TwinDBBackupConfig(object):
    """
    Class represents TwinDB Backup configuration
    """
    def __init__(self, config_file=DEFAULT_CONFIG_FILE_PATH):
        self._config_file = config_file
        self.__cfg = ConfigParser()
        self.__cfg.read(self._config_file)
        self.__mysql = None

    @property
    def retention(self):
        """
        :return: Remote retention policy.
        :rtype: RetentionPolicy
        """
        return self._retention('retention')

    @property
    def retention_local(self):
        """
        :return: Local retention policy.
        :rtype: RetentionPolicy
        """
        return self._retention('retention_local')

    @property
    def run_intervals(self):
        """
        Run intervals config. When to run or not the backup.

        :return: Configuration with data on whether to run the backup tool now.
        :rtype: RunIntervals
        """
        kwargs = {}
        for i in INTERVALS:
            option = 'run_%s' % i
            try:
                kwargs[i] = self.__cfg.getboolean('intervals', option)
            except (NoOptionError, NoSectionError):
                LOG.debug(
                    'Option %s is not defined in section intervals',
                    option
                )
        return RunIntervals(**kwargs)

    @property
    def mysql(self):
        """
        :return: Local MySQL source configuration.
        :rtype: MySQLConfig
        """
        if self.__mysql is None:
            kwargs = {}
            try:
                options = [
                    'mysql_defaults_file',
                    'full_backup',
                    'expire_log_days',
                    'xtrabackup_binary',
                    'xbstream_binary'
                ]
                for opt in options:
                    try:
                        kwargs[opt] = self.__cfg.get('mysql', opt).strip('"\'')
                    except NoOptionError:
                        LOG.debug(
                            'Option %s is not defined in section mysql',
                            opt
                        )
                self.__mysql = MySQLConfig(**kwargs)

            except NoSectionError:
                return None

        return self.__mysql

    @property
    def ssh(self):
        """
        :return: Remote SSH configuration.
        :rtype: SSHConfig
        """
        kwargs = {}
        try:
            options = [
                'ssh_user',
                'ssh_key',
                'port',
                'backup_host',
                'backup_dir'
            ]
            for opt in options:
                try:
                    kwargs[opt] = self.__cfg.get('ssh', opt).strip('"\'')
                except NoOptionError:
                    LOG.debug('Option %s is not defined in section ssh', opt)
            return SSHConfig(**kwargs)

        except NoSectionError:
            return None

    @property
    def s3(self):  # pylint: disable=invalid-name
        """Amazon S3 configuration"""
        kwargs = {}
        try:
            options = [
                'aws_access_key_id',
                'aws_secret_access_key',
                'aws_default_region',
                'bucket'
            ]
            for opt in options:
                try:
                    kwargs[opt] = self.__cfg.get('s3', opt).strip('"\'')
                except NoOptionError:
                    LOG.debug('Option %s is not defined in section s3', opt)
            return S3Config(**kwargs)

        except NoSectionError:
            return None

    @property
    def keep_local_path(self):
        """If specified a local path where
        the tool will keep an additional local backup copy.
        """
        try:
            return self.__cfg.get('destination', 'keep_local_path')
        except (NoSectionError, NoOptionError):
            return None

    @property
    def exporter(self):
        """
        Read config and return export transport instance

        :return: Instance of export transport, if it is set
        :rtype: BaseExporter
        :raise: ConfigurationError, if transport isn't implemented
        """
        try:
            try:
                transport = self.__cfg.get("export", "transport")
                if transport == "datadog":
                    app_key = self.__cfg.get("export", "app_key")
                    api_key = self.__cfg.get("export", "api_key")
                    return DataDogExporter(app_key, api_key)
                else:
                    raise ConfigurationError(
                        'Metric exported \'%s\' is not implemented'
                        % transport
                    )
            except NoOptionError as err:
                raise ConfigurationError(err)

        except NoSectionError:
            return None

    @property
    def gpg(self):
        """GPG configuration."""
        kwargs = {}
        try:
            for opt in ['secret_keyring']:
                try:
                    kwargs[opt] = self.__cfg.get('gpg', opt).strip('"\'')
                except NoOptionError:
                    LOG.debug('Option %s is not defined in section s3', opt)
            return GPGConfig(
                self.__cfg.get('gpg', 'recipient').strip('"\''),
                self.__cfg.get('gpg', 'keyring').strip('"\''),
                **kwargs
            )

        except NoSectionError:
            return None

    @property
    def backup_dirs(self):
        """Directories to backup"""
        try:
            dirs = self.__cfg.get('source', 'backup_dirs')
            return split(dirs)
        except NoOptionError:
            return []
        except NoSectionError as err:
            raise ConfigurationError(err)

    @property
    def backup_mysql(self):
        """FLag to backup MySQL or not"""
        try:
            return self.__cfg.getboolean('source', 'backup_mysql')
        except NoOptionError:
            return False
        except NoSectionError as err:
            raise ConfigurationError(err)

    def destination(self, backup_source=socket.gethostname()):
        """
        :param backup_source: Hostname of the host where backup is taken from.
        :type backup_source: str
        :return: Backup destination instance
        :rtype: BaseDestination
        """
        try:
            backup_destination = self.__cfg.get(
                'destination',
                'backup_destination'
            )
            if backup_destination == 'ssh':
                return Ssh(
                    self.ssh.path,
                    hostname=backup_source,
                    ssh_host=self.ssh.host,
                    ssh_port=self.ssh.port,
                    ssh_user=self.ssh.user,
                    ssh_key=self.ssh.key,
                )
            elif backup_destination == 's3':
                return S3(
                    bucket=self.s3.bucket,
                    aws_access_key_id=self.s3.aws_access_key_id,
                    aws_secret_access_key=self.s3.aws_secret_access_key,
                    aws_default_region=self.s3.aws_default_region,
                    hostname=backup_source
                )

            else:
                raise ConfigurationError(
                    'Unsupported destination \'%s\''
                    % backup_destination
                )
        except NoSectionError:
            raise ConfigurationError(
                '%s is missing required section \'destination\''
                % self._config_file
            )

    def _retention(self, section):
        kwargs = {}
        for i in INTERVALS:
            option = '%s_copies' % i
            try:
                kwargs[i] = self.__cfg.getint(section, option)
            except (NoOptionError, NoSectionError):
                LOG.warning(
                    'Option %s is not defined in section %s',
                    option,
                    section
                )
        return RetentionPolicy(**kwargs)
