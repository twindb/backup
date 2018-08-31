# -*- coding: utf-8 -*-
"""
Module defines Base destination class and destination exception(s).
"""
import re
from abc import abstractmethod

from subprocess import Popen, PIPE

from twindb_backup import LOG
from twindb_backup.destination.exceptions import DestinationError
from twindb_backup.status.exceptions import CorruptedStatus


class BaseDestination(object):
    """Base destination class"""

    def __init__(self, remote_path, status_path=None):
        if not remote_path:
            raise DestinationError(
                'remote path must be defined and cannot be %r' % remote_path
            )
        self.remote_path = remote_path.rstrip('/')
        if status_path:
            self.status_path = status_path
        else:
            self.status_path = '%s/status' % remote_path

    @abstractmethod
    def save(self, handler, name):
        """
        Save the given stream.

        :param handler: Incoming stream.
        :type handler: file
        :param name: Save stream as this name.
        :type name: str
        :raise: DestinationError if any error
        """

    @staticmethod
    def _save(cmd, handler):

        with handler as input_handler:
            LOG.debug('Running %s', ' '.join(cmd))
            try:
                proc = Popen(cmd, stdin=input_handler,
                             stdout=PIPE,
                             stderr=PIPE)
                cout_ssh, cerr_ssh = proc.communicate()

                ret = proc.returncode
                if ret:
                    if cout_ssh:
                        LOG.info(cout_ssh)
                    if cerr_ssh:
                        LOG.error(cerr_ssh)
                    raise DestinationError('%s exited with error code %d'
                                           % (' '.join(cmd), ret))
                LOG.debug('Exited with code %d', ret)
            except OSError as err:
                raise DestinationError('Failed to run %s: %s'
                                       % (' '.join(cmd), err))

    def list_files(self,
                   prefix,
                   recursive=False,
                   pattern=None,
                   files_only=False):
        """
        Get list of file by prefix

        :param prefix: Path
        :param recursive: Recursive return list of files
        :type prefix: str
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
                    prefix,
                    recursive=recursive,
                    files_only=files_only
                ),
                pattern=pattern
            )
        )

    @abstractmethod
    def _list_files(self, path, recursive=False, files_only=False):
        raise NotImplementedError

    @abstractmethod
    def delete(self, obj):
        """
        Delete object from the destination

        :param obj:
        :return:
        """

    def status(self, status=None):
        """
        Read or save backup status. Status is an instance of Status class.
        If status is None the function will read status from
        the remote storage.
        Otherwise it will store the status remotely.

        :param status: instance of Status class
        :type status: Status
        :return: instance of Status class
        :rtype: Status
        """
        def _write_retry(status_content, attempts=3):
            for _ in xrange(attempts):
                self._write_status(status_content)
                try:
                    return self._read_status()
                except CorruptedStatus:
                    pass
            raise DestinationError("Can't write status")
        if status:
            return _write_retry(status, attempts=3)
        else:
            return self._read_status()

    @abstractmethod
    def _write_status(self, status):
        """Function that actually writes status"""

    @abstractmethod
    def _read_status(self):
        """Function that actually reads status"""

    @abstractmethod
    def _status_exists(self):
        """Check if status file exists"""

    def get_run_type_from_full_path(self, path):
        """
        For a given backup copy path find what run_type it was.
        For example,
        s3://bucket/ip-10-0-52-101/daily/files/_etc-2018-04-05_00_07_13.tar.gz
        is a daily backup and
        /path/to/twindb-server-backups/master1/hourly/
        mysql/mysql-2018-04-08_03_51_18.xbstream.gz
        is an hourly backup.

        :param path: Full path of the backup copy
        :return: Run type this backup was taken on:

            - hourly
            - daily
            - weekly
            - monthly
            - yearly
        :rtype: str
        """
        return self.basename(path).split('/')[1]

    def basename(self, filename):
        """
        Basename of backup copy

        :param filename:
        :return:
        """
        return filename.replace(self.remote_path + '/', '', 1)

    def get_latest_backup(self):
        """Get latest backup path"""
        cur_status = self.status()
        latest = cur_status.get_latest_backup()
        if latest is None:
            return None
        url = "{remote_path}/{filename}".format(
            remote_path=self.remote_path,
            filename=latest
        )
        return url

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
