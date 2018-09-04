"""Base status is a class for a general purpose status.
"""
import json
import hashlib
from abc import abstractmethod, abstractproperty
from base64 import b64decode

from twindb_backup import STATUS_FORMAT_VERSION
from twindb_backup.status.exceptions import CorruptedStatus, StatusKeyNotFound


class BaseStatus(object):
    """Base class for status.

    :param content: if passed it will initialize a status from this string.
    :type content: str
    :raise CorruptedStatus: If the content string is not a valid status
        or empty string.
    """
    __version__ = STATUS_FORMAT_VERSION

    @property
    def md5(self):
        """
        MD5 checksum of the status. It is calculated as a md5 of output of
        ``self._status_serialize()``.
        """
        return hashlib.md5(
            self._status_serialize()
        ).hexdigest()

    def __init__(self, content=None):
        if content == '':
            raise CorruptedStatus('Status content cannot be an empty string')
        try:
            status = json.loads(content)
            md5_stored = status['md5']
            md5_calculated = hashlib.md5(
                status['status']
            ).hexdigest()
            if md5_calculated != md5_stored:
                raise CorruptedStatus('Checksum mismatch')

            self._status = self._load(
                b64decode(
                    status['status']
                )
            )
            self._status.sort(
                key=lambda cp: cp.created_at
            )
        except TypeError:  # Init from None
            self._status = []
        except ValueError:  # Old format
            self._status = self._load(
                b64decode(content)
            )
            self._status.sort(
                key=lambda cp: cp.created_at
            )

    @property
    def version(self):
        """
        Version of status file. Originally status file didn't have
        any versions, but in future the version will be used to work
        with new features.
        """
        return self.__version__

    def add(self, backup_copy):
        """
        Add entry to status.

        :param backup_copy: Instance of backup copy
        :type backup_copy: BaseCopy
        """
        self._status.append(backup_copy)

    def remove(self, key):
        """
        Remove key from the status.
        """
        copy = self[key]
        self._status.remove(copy)

    def serialize(self):
        """
        Return a string that represents current state
        """
        return json.dumps(
            {
                "status": self._status_serialize(),
                "version": self.version,
                "md5": self.md5
            },
            sort_keys=True
        )

    def _status_serialize(self):
        raise NotImplementedError

    def get_latest_backup(self):
        """
        Find the latest backup copy.

        :return: backup copy
        :rtype: BaseCopy
        """
        try:
            return self._status[len(self._status) - 1]
        except IndexError:
            return None

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._status[item]
        elif isinstance(item, (str, unicode)):
            for copy in self._status:
                if copy.key == str(item):
                    return copy
            raise StatusKeyNotFound('Copy %s not found' % item)
        else:
            raise NotImplementedError('Type %s not supported' % type(item))

    def __str__(self):
        return b64decode(
            self._status_serialize()
        )

    def __len__(self):
        return len(self._status)

    @abstractmethod
    def _load(self, status_as_json):
        """
        Parse status_as_json string and construct a status.

        :param status_as_json: A JSON string with status
        :type status_as_json: str
        :return: status object - list of BackupCopies
        :rtype: list
        """
        raise NotImplementedError

    @abstractproperty
    def basename(self):
        """
        Returns file name where the status is store in the destination.
        """
