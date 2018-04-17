"""Class to describe MySQL backup copy"""
import json
from base64 import b64encode
from pprint import pformat

from twindb_backup.copy.base_copy import BaseCopy
from twindb_backup.copy.exceptions import WrongInputData


class MySQLCopy(BaseCopy):  # pylint: disable=too-many-instance-attributes
    """
    Instantiate a MySQL copy.

    :param host: Hostname where the backup was taken from.
    :type host: str
    :param run_type: Run type when the backup was taken: daily, weekly, etc.
    :type run_type: str
    :param name: Base name of the backup copy file as it's stored
        on the destination.
    :type name: str
    :raise WrongInputData: if type is neither full or incremental,
        if name is not a basename.
    """
    def __init__(self, host, run_type, name, **kwargs):
        super(MySQLCopy, self).__init__(host, run_type, name)

        self._source_type = 'mysql'

        if 'type' in kwargs and kwargs.get('type') in ['full', 'incremental']:
            self._type = kwargs.get('type')
        else:
            raise WrongInputData('Type of MySQL backup copy is mandatory.'
                                 ' Can be either full or incremental')

        if '/' in name:
            raise WrongInputData(
                'name must be relative, without any slashes.'
                ' Got %s instead.' % name)

        self._backup_started = int(kwargs.get('backup_started', 0))
        self._backup_finished = int(kwargs.get('backup_finished', 0))
        self._binlog = kwargs.get('binlog', None)
        self._position = kwargs.get('position', 0)
        self._lsn = kwargs.get('lsn', 0)
        self._parent = kwargs.get('parent', None)

        if 'wsrep_provider_version' in kwargs:
            self._galera = True
            self._wsrep_provider_version = kwargs.get('wsrep_provider_version')
        else:
            self._galera = False
            self._wsrep_provider_version = None

        if 'config' in kwargs and 'config_files' in kwargs:
            raise WrongInputData(
                'Either config or config_files can be used '
                'to initialize config attribute')

        if 'config_files' in kwargs:
            self._config = []
            config_files = kwargs.get('config_files', [])
            for config_file in config_files:
                with open(config_file) as config_descr:
                    self._config.append(
                        {
                            config_file: config_descr.read()
                        }
                    )
        else:
            self._config = kwargs.get('config', [])

    def __eq__(self, other):
        comparison = ()
        for attr in ['host', 'run_type', 'name',
                     'binlog', 'position',
                     'lsn',
                     'parent', 'wsrep_provider_version',
                     'config']:
            comparison += (getattr(self, attr) == getattr(other, attr), )
            if getattr(self, attr) != getattr(other, attr):
                print(
                    '%s differs: %s != %s'
                    % (
                        attr,
                        getattr(self, attr),
                        getattr(other, attr)
                    )
                )

        # Compare float type time values as integers
        for attr in ['backup_started', 'backup_finished']:
            comparison += (
                int(getattr(self, attr)) == int(getattr(other, attr)),
            )
        return all(comparison)

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def host(self):
        """Host where the backup was taken"""
        return self._host

    @property
    def name(self):
        """Name of the backup. It's basename w/o directory part."""
        return self._name

    @property
    def backup_started(self):
        """Timestamp when the backup job started."""
        return self._backup_started

    @property
    def backup_finished(self):
        """Timestamp when the backup job finished."""
        return self._backup_finished

    @property
    def duration(self):
        """Time in seconds it took to take the backup."""
        return self._backup_finished - self._backup_started

    @property
    def binlog(self):
        """File name of the binlog."""
        return self._binlog

    @property
    def position(self):
        """Binlog position of the backup copy."""
        return self._position

    @property
    def type(self):
        """Full or incremental."""
        return self._type

    @property
    def parent(self):
        """For incremental backup it is a base full copy name."""
        return self._parent

    @property
    def lsn(self):
        """LSN of the backup."""
        return self._lsn

    @property
    def config(self):
        """List of configs and their content."""
        return self._config

    @property
    def galera(self):
        """True if the backup was taken from Galera."""
        return self._galera

    @property
    def wsrep_provider_version(self):
        """If it was Galera, value of wsrep_provider_version"""
        return self._wsrep_provider_version

    def as_dict(self):
        """Return representation of the class instance for output purposes."""
        result = {}
        for attr in ['type',
                     'binlog', 'position',
                     'backup_started', 'backup_finished',
                     'lsn',
                     'parent',
                     'galera', 'wsrep_provider_version']:
            result[attr] = getattr(self, attr)
            encoded_configs = []
            for config in self.config:
                config_dict = {}
                for name, content in config.iteritems():
                    config_dict[name] = b64encode(content)
                encoded_configs.append(config_dict)
            result['config'] = encoded_configs
        return result

    def __str__(self):
        json_view = json.dumps(self.as_dict())
        return pformat(json_view)
