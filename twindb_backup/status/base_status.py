"""Base status is a class for a general purpose status.
"""
import hashlib
import json
import socket
from abc import abstractmethod, abstractproperty
from base64 import b64decode
from os import path as osp

from twindb_backup import LOG, STATUS_FORMAT_VERSION
from twindb_backup.destination.exceptions import FileNotFound
from twindb_backup.status.exceptions import CorruptedStatus, StatusKeyNotFound


class BaseStatus(object):
    """Base class for status. It can be instantiated either from
    a string with status content or from a destination instance.
    If the destination is given then the status will be read from a status
    file on the destination.

    :param content: if passed it will initialize a status from this string.
    :type content: str
    :param dst: Destination instance.
    :type dst: BaseDestination
    :param status_directory: Relative path to a directory where the status
        file is stored. Usually,
        it's a hostname where backup was taken from.
    :type status_directory: str
    :raise CorruptedStatus: If the content string is not a valid status
        or empty string.
    """

    __version__ = STATUS_FORMAT_VERSION

    def __init__(self, content=None, dst=None, status_directory=None):
        self._status_directory = status_directory or socket.gethostname()
        self._status = []
        if dst:
            self.__init_from_str(self._read(dst))
        else:
            self.__init_from_str(content)

    @abstractproperty
    def basename(self):
        """
        Returns file name without a directory path
        where the status is stored in the destination.
        """
        return "status"

    @property
    def latest_backup(self):
        """
        Find the latest backup copy.

        :return: backup copy or None if status is empty.
        :rtype: BaseCopy
        """
        try:
            return self._status[len(self._status) - 1]
        except IndexError:
            return None

    @property
    def md5(self):
        """
        :return: MD5 checksum of the status. It is calculated as
            a md5 of output of ``self._status_serialize()``.
        :rtype: str
        """
        return hashlib.md5(self._status_serialize().encode("utf-8")).hexdigest()

    @property
    def status_path(self):
        """
        Return relative path where status is stored.

        :return: relative to the destination path where the status is stored.
        :rtype: str
        """
        return osp.join(self._status_directory, self.basename)

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

        :param key: A copy key in the status.
        :type key: str
        """
        copy = None
        try:
            copy = self[key]
            self._status.remove(copy)
        except StatusKeyNotFound:
            for copy in self._status:
                if key.endswith(copy.key):
                    self._status.remove(copy)
                    return
            raise

    def serialize(self):
        """
        Return a string that represents current state
        """
        return json.dumps(
            {
                "status": self._status_serialize(),
                "version": self.version,
                "md5": self.md5,
            },
            sort_keys=True,
        )

    def save(self, dst):
        """
        Write status file to the destination.

        :param dst: Destination instance.
        :type dst: BasicDestination
        """
        dst.write(self.serialize(), self.status_path)

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

    def _read(self, dst):
        """
        Read status file from the destination.

        :param dst: Destination instance.
        :type dst: BasicDestination
        :return: Content of the file or None if file doesn't exist.
        :rtype: str
        """
        try:
            return dst.read(self.status_path)
        except (FileNotFound, FileNotFoundError):
            return None

    def _status_serialize(self):
        raise NotImplementedError

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._status[item]
        elif isinstance(item, (str,)):
            for copy in self._status:
                if copy.key == str(item):
                    return copy
            raise StatusKeyNotFound("Copy %s not found" % item)
        else:
            raise NotImplementedError("Type %s not supported" % type(item))

    def __str__(self):
        return b64decode(self._status_serialize()).decode("utf-8")

    def __len__(self):
        return len(self._status)

    def __init_from_str(self, content):
        """Initialize status from a string."""
        if content == "":
            raise CorruptedStatus("Status content cannot be an empty string")
        try:
            status = json.loads(content)
            md5_stored = status["md5"]
            md5_calculated = hashlib.md5(status["status"].encode("utf-8")).hexdigest()
            if md5_calculated != md5_stored:
                raise CorruptedStatus("Checksum mismatch")

            self._status = self._load(b64decode(status["status"]).decode("utf-8"))
            self._status.sort(key=lambda cp: cp.created_at, reverse=True)
        except TypeError:  # Init from None
            self._status = []
        except ValueError as err:  # Old format
            LOG.debug(err)
            LOG.debug("Looks like old format")
            self._status = self._load(b64decode(content).decode("utf-8"))
            LOG.debug("Loaded status: %s", self._status)
            self._status.sort(key=lambda cp: cp.sort_key)
