"""Class to store and work with status file"""
from __future__ import print_function
import json
from base64 import b64decode, b64encode

from os.path import basename

from twindb_backup import INTERVALS, LOG
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.base_status import BaseStatus
from twindb_backup.status.exceptions import CorruptedStatus, \
    StatusError, StatusKeyNotFound


# For backward compatibility content of my.cnf files is base64 encoded.
def _decode_mycnf(_json):
    for interval in INTERVALS:
        for bcopy in _json[interval]:
            if "config" in _json[interval][bcopy]:
                _json[interval][bcopy]["config"] = \
                    _deserialize_config_dict(
                        _json[interval][bcopy]["config"]
                    )
    return _json


def _encode_mycnf(status):
    for interval in INTERVALS:
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


class MySQLStatus(BaseStatus):
    """
    Class that stores status file and implements operations on it.
    """
    def __init__(self, content=None, version=None):
        super(MySQLStatus, self).__init__()

        self.__version__ = version if version else 0
        if content:
            try:
                _json = json.loads(b64decode(content))
                _json = _decode_mycnf(_json)
            except TypeError as err:
                LOG.debug('Corrupted status content: %s', content)
                raise CorruptedStatus('Corrupted status: %s', err.message)

            for i in INTERVALS:
                for key, value in _json[i].iteritems():
                    try:
                        host = key.split('/')[0]
                        run_type = key.split('/')[1]
                    except IndexError:
                        raise StatusError(
                            'Failed to detect host or run_type from %s' % key
                        )
                    name = basename(key)

                    getattr(self, i)[key] = MySQLCopy(
                        host,
                        run_type,
                        name,
                        **value
                    )

    def next_backup_type(self, full_backup, run_type):
        """
        Return backup type to take. If full_backup=daily then
        for hourly backups it will be incremental, for all other - full

        :param full_backup: when to take full backup according to config.
        :param run_type: what kind of backup run it is.
        :return: "full" or "incremental"
        :rtype: str
        """
        if INTERVALS.index(full_backup) <= INTERVALS.index(run_type):
            return "full"
        elif not self.full_copy_exists(run_type):
            return "full"
        else:
            return "incremental"

    def eligble_parent(self, run_type):
        """
        Find a backup copy that can be a parent

        :param run_type: See :func:`~get_backup_type`.
        :return: Backup copy or None
        :rtype: MySQLCopy
        """
        full_backup_index = INTERVALS.index(run_type)
        for i in xrange(full_backup_index, len(INTERVALS)):
            period_copies = getattr(self, "_%s" % INTERVALS[i])
            for _, value in period_copies.iteritems():
                try:
                    if value.type == 'full':
                        return value
                except KeyError:
                    return None
        return None

    def full_copy_exists(self, run_type):
        """
        Check whether there is a full copy.

        :param run_type: See :func:`~get_backup_type`.
        :return: True if there is a full copy. False if there is no
            an eligible full copy.
        :rtype: bool
        """
        return self.eligble_parent(run_type) is not None

    def get_full_copy_name(self, run_type, name):
        """
        Return path to full copy name

        :param run_type: Run type
        :param name: Basename
        :return: Path to full copy
        """
        period_copies = getattr(self, run_type)
        for key, value in period_copies.iteritems():
            if name in key:
                return value.parent
        raise StatusKeyNotFound("Full copy not found")
