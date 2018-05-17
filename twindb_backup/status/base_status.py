"""Base status is a class for a general purpose status.
"""
import json
import hashlib
from abc import abstractproperty, abstractmethod
from base64 import b64encode

from twindb_backup import STATUS_FORMAT_VERSION


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
