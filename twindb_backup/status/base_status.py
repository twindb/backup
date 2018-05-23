"""Base status is a class for a general purpose status.
"""
import json
import hashlib
from abc import abstractproperty, abstractmethod
from base64 import b64encode, b64decode

from twindb_backup import STATUS_FORMAT_VERSION, LOG
from twindb_backup.status.exceptions import CorruptedStatus
from twindb_backup.util import normalize_b64_data


def _parse_status(content):
    raw_json = json.loads(content)
    md5_hash = hashlib.md5(raw_json["status"].encode()).hexdigest()
    if md5_hash != raw_json["md5"]:
        raise CorruptedStatus('Corrupted status: %s', content)
    _json = json.loads(b64decode(normalize_b64_data(raw_json["status"])))
    return raw_json["version"], _json


class BaseStatus(object):
    """Base class for status."""
    __version__ = STATUS_FORMAT_VERSION

    @abstractproperty
    def valid(self):
        """
        Returns True if status is valid.
        """

    @property
    def version(self):
        """
        Version of status file. Originally status file didn't have
        any versions, but in future the version will be used to work
        with new features.
        """
        return self.__version__

    @abstractmethod
    def add(self, backup_copy):
        """
        Add entry to status.

        :param backup_copy: Instance of backup copy
        :type backup_copy: BaseCopy
        :return: Nothing
        """

    @abstractmethod
    def remove(self, key, period=None):
        """
        Remove key from the status.
        """

    def serialize(self):
        """
        Return a string that represents current state
        """
        encoded_status = b64encode(self.__str__())
        status_md5 = hashlib.md5(encoded_status.encode())

        return json.dumps(
            {
                "status": encoded_status,
                "version": STATUS_FORMAT_VERSION,
                "md5": status_md5.hexdigest()
            }, sort_keys=True)

    @abstractmethod
    def get_latest_backup(self):
        """
        Find the latest backup copy.

        :return: backup copy
        :rtype: BaseCopy
        """

    def __getitem__(self, item):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    @staticmethod
    def valid_content(content):
        """
        Validate content on valid JSON.

        :param content: Encoded JSON
        :return: Tuple of version and decoded status
        :rtype: tuple
        :raise CorruptedStatus: If status corrupted
        """
        if content:
            try:
                version, _json = _parse_status(content)
            except (TypeError, ValueError):
                try:
                    _json = json.loads(b64decode(normalize_b64_data(content)))
                    version = 0
                except (TypeError, ValueError) as err:
                    LOG.debug('Corrupted status content: %s', content)
                    raise CorruptedStatus('Corrupted status: %s', err.message)
            return version, _json
        else:
            return None, None
