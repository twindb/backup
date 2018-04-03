"""Class to store and work with status file"""
from __future__ import print_function
import json
from base64 import b64decode, b64encode
from copy import deepcopy

from twindb_backup.status.exceptions import StatusKeyNotFound


# For backward compatibility content of my.cnf files is base64 encoded.
def _decode_mycnf(_json):
    for interval in ["hourly", "daily", "weekly", "monthly", "yearly"]:
        for bcopy in _json[interval]:
            if "config" in _json[interval][bcopy]:
                _json[interval][bcopy]["config"] = \
                    _deserialize_config_dict(
                        _json[interval][bcopy]["config"]
                    )
    return _json


def _encode_mycnf(status):
    for interval in ["hourly", "daily", "weekly", "monthly", "yearly"]:
        for bcopy in status[interval]:
            if "config" in status[interval][bcopy]:
                status[interval][bcopy]["config"] = \
                    _serialize_config_dict(
                        status[interval][bcopy]["config"]
                    )
    return status


def _serialize_config_dict(config):
    config_serialized = []
    for cnf in config:
        for name, cnf_content in cnf.iteritems():
            config_serialized.append(
                {
                    name: b64encode(cnf_content)
                }
            )
    return config_serialized


def _deserialize_config_dict(config):
    config_deserialized = []
    for cnf in config:
        for name, cnf_content in cnf.iteritems():
            config_deserialized.append(
                {
                    name: b64decode(cnf_content)
                }
            )
    return config_deserialized


class Status(object):
    """
    Class that stores status file and implements operations on it.
    """
    __version__ = None
    _hourly = {}
    _daily = {}
    _weekly = {}
    _monthly = {}
    _yearly = {}

    def __init__(self, content=None, version=None):
        self.__version__ = version if version else 0
        if content:
            _json = json.loads(b64decode(content))
            _json = _decode_mycnf(_json)

            self._hourly = _json["hourly"]
            self._daily = _json["daily"]
            self._weekly = _json["weekly"]
            self._monthly = _json["monthly"]
            self._yearly = _json["yearly"]

    def __eq__(self, other):
        if isinstance(other, dict):
            return all(
                (
                    self._hourly == other['hourly'],
                    self._daily == other['daily'],
                    self._weekly == other['weekly'],
                    self._monthly == other['monthly'],
                    self._yearly == other['yearly'],
                )
            )
        elif isinstance(other, Status):
            print('Comparing two statuses')
            print(self)
            print(other)
            return all(
                (
                    self._hourly == other.hourly,
                    self._daily == other.daily,
                    self._weekly == other.weekly,
                    self._monthly == other.monthly,
                    self._yearly == other.yearly,
                )
            )
        else:
            return False

    def __repr__(self):
        return json.dumps(
            {
                "hourly": self._hourly,
                "daily": self._daily,
                "weekly": self._weekly,
                "monthly": self._monthly,
                "yearly": self._yearly
            }
        )

    @property
    def valid(self):
        """
        Returns True if status is valid.
        """
        return all(
            (
                self._hourly is not None,
                self._daily is not None,
                self._weekly is not None,
                self._monthly is not None,
                self._yearly is not None
            )
        )

    @property
    def version(self):
        """
        Version of status file. Originally status file didn't have
        any versions, but in future the version will be used to work
        with new features.
        """
        return self.__version__

    @property
    def hourly(self):
        """Dictionary with hourly backups"""
        return self._hourly

    @property
    def daily(self):
        """Dictionary with daily backups"""
        return self._daily

    @property
    def weekly(self):
        """Dictionary with weekly backups"""
        return self._weekly

    @property
    def monthly(self):
        """Dictionary with monthly backups"""
        return self._monthly

    @property
    def yearly(self):
        """Dictionary with yearly backups"""
        return self._yearly

    def add(self, period, key, **kwargs):
        """
        Add entry to status.

        :param period: one of 'hourly', 'daily', 'weekly', 'monthly', 'yearly'
        :type period: str
        :param key: Backup name in status. It's a relative file name
            of a backup copy. For example,
            master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz
        :type key: str
        :param kwargs: Keyword arguments for status entry
        :return: Nothing
        """
        backup_type = kwargs.get('type', 'full')
        getattr(self, "_%s" % period)[key] = {
            'binlog': kwargs.get('binlog', None),
            'position': kwargs.get('position', 0),
            'lsn': kwargs.get('lsn', None),
            'type': backup_type,
            'backup_started': kwargs.get('backup_started', None),
            'backup_finished': kwargs.get('backup_finished', None),
            'config': kwargs.get('config', [])
        }
        if backup_type == 'incremental':
            parent = kwargs.get('parent', None)
            getattr(self, "_%s" % period)[key]['parent'] = parent

        if kwargs.get('galera', False):
            wsrep_provider_version = kwargs.get('wsrep_provider_version', None)
            getattr(self, "_%s" % period)[key]['wsrep_provider_version'] \
                = wsrep_provider_version

    def remove(self, period, key):
        """
        Remove key from the status.

        :param period: one of 'hourly', 'daily', 'weekly', 'monthly', 'yearly'
        :type period: str
        :param key: Backup name in status. It's a relative file name
            of a backup copy. For example,
            master1/daily/mysql/mysql-2018-03-28_04_09_53.xbstream.gz
        :type key: str
        :raise StatusKeyNotFound: if there is no such key in the status
        """
        try:
            del getattr(self, "_%s" % period)[key]
        except KeyError:
            raise StatusKeyNotFound(
                "There is no %s in %s backups"
                % (period, key)
            )

    def serialize(self):
        """Return a string that represents current state
        """
        status = {
            'hourly': deepcopy(self._hourly),
            'daily': deepcopy(self._daily),
            'weekly': deepcopy(self._weekly),
            'monthly': deepcopy(self._monthly),
            'yearly': deepcopy(self._yearly)
        }
        status = _encode_mycnf(status)
        return b64encode(json.dumps(status))

    @staticmethod
    def deserialize(content):
        """Create Status instance from status file content."""
        return Status(content)
