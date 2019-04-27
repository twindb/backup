# -*- coding: utf-8 -*-
"""
Module defines Base destination class and destination exception(s).
"""
import re
from abc import abstractmethod

from twindb_backup import LOG
from twindb_backup.destination.exceptions import DestinationError
from twindb_backup.exceptions import TwinDBBackupInternalError


class BaseDestination(object):
    """Base destination class"""

    def __init__(self, remote_path):
        if not remote_path:
            raise DestinationError(
                'remote path must be defined and cannot be %r' % remote_path
            )
        self.remote_path = remote_path.rstrip('/')

    @abstractmethod
    def delete(self, path):
        """
        Delete object from the destination

        :param path: Relative path to the file to delete
        """

    @abstractmethod
    def get_stream(self, copy):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination.

        :param copy: Backup copy
        :type copy: BaseCopy
        :return: Standard output.
        """

    def list_files(self,
                   prefix=None,
                   recursive=False,
                   pattern=None,
                   files_only=False):
        """
        Get list of file by prefix.

        :param prefix: Path
        :type prefix: str
        :param recursive: Recursive return list of files
        :type recursive: bool
        :param pattern: files must match with this regexp if specified
        :type pattern: str
        :param files_only: If True don't list directories
        :type files_only: bool
        :return: List of files
        :rtype: list
        """
        return sorted(
            self._match_files(
                self._list_files(
                    prefix=prefix,
                    recursive=recursive,
                    files_only=files_only
                ),
                pattern=pattern
            )
        )

    @abstractmethod
    def read(self, filepath):
        """
        Read content of a file path from destination.

        :param filepath: Relative path to file.
        :type filepath: str
        :return: Content of the file.
        :rtype: str
        """
        raise TwinDBBackupInternalError(
            'Method read() is not implemented in %s'
            % self.__class__
        )

    @abstractmethod
    def save(self, handler, filepath):
        """
        Save a stream given as handler to filepath.

        :param handler: Incoming stream.
        :type handler: file
        :param filepath: Save stream as this name.
        :type filepath: str
        """
        raise TwinDBBackupInternalError(
            'Method save() is not implemented in %s'
            % self.__class__
        )

    @abstractmethod
    def write(self, content, filepath):
        """
        Write ``content`` to a file.

        :param content: Content to write to the file.
        :type content: str
        :param filepath: Relative path to file.
        :type filepath: str
        """
        raise TwinDBBackupInternalError(
            'Method write() is not implemented in %s'
            % self.__class__
        )

    @abstractmethod
    def _list_files(self, prefix=None, recursive=False, files_only=False):
        """
        A descendant class must implement this method.
        It should return a list of files already filtered out by prefix.
        Some storage engines (e.g. Google Cloud Storage) allow that
        at the API level. The method should use storage level filtering
        to save on network transfers.
        """
        raise NotImplementedError

    @staticmethod
    def _match_files(files, pattern=None):
        LOG.debug('Pattern: %s', pattern)
        LOG.debug('Unfiltered files: %r', files)
        result = []
        for fil in files:
            if pattern:
                if re.search(pattern, fil):
                    result.append(fil)
            else:
                result.append(fil)
        LOG.debug('Filtered files: %r', result)
        return result
