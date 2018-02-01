# -*- coding: utf-8 -*-
"""
Module defines Base destination class and destination exception(s).
"""
import base64
import hashlib
import json
from abc import abstractmethod

from subprocess import Popen, PIPE

import time

from twindb_backup import LOG, INTERVALS
from twindb_backup.destination.exceptions import DestinationError, \
    StatusFileError


class BaseDestination(object):
    """Base destination class"""

    def __init__(self):
        self.remote_path = ''
        self.status_path = ''
        self.status_tmp_path = ''

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
                return ret == 0
            except OSError as err:
                raise DestinationError('Failed to run %s: %s'
                                       % (' '.join(cmd), err))

    @abstractmethod
    def list_files(self, prefix, recursive=False):
        """
        List files

        :param prefix:
        :param recursive:
        """

    @abstractmethod
    def find_files(self, prefix, run_type):
        """
        Find files

        :param prefix:
        :param run_type:
        :return:
        """

    @abstractmethod
    def delete(self, obj):
        """
        Delete object from the destination

        :param obj:
        :return:
        """

    @property
    def _empty_status(self):
        return {
            'hourly': {},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'yearly': {}
        }

    def status(self, status=None):
        """
        Read or save backup status. Status is a dictionary with available
        backups and their properties. If status is None the function
        will read status from the remote storage.
        Otherwise it will store the status remotely.

        :param status: Dictionary with status

        ::

            {
                'hourly': [
                    {
                        'filename': '/remote/path',
                        'binlog': 'mysql-bin.000001',
                        'position': 43670
                    }
                ],
                'checksum': 'c2a8ed7ddbf759a67b2d5ea256f05fb8'
            }
        :type status: dict
        :return: dictionary with the status
        """
        if status:
            status['checksum'] = hashlib.md5(
                json.dumps(
                    status, sort_keys=True
                )
            ).hexdigest()
            raw_status = base64.b64encode(json.dumps(status, sort_keys=True))
            self._write_status(raw_status)
        else:
            return self._read_status()

    @abstractmethod
    def _write_status(self, status):
        """Function that actually writes status"""

    def _read_status(self):
        if not self._status_exists():
            return self._empty_status
        i = 1
        while not self._is_valid_status(self.status_path):
            if i == 4:
                break
            time.sleep(3 * i)
            i = i + 1
        if i < 4:
            return self._get_pretty_status(self.status_path)
        if self._is_valid_status(self.status_tmp_path):
            self._move_file(self.status_tmp_path, self.status_path)
            return self._get_pretty_status(self.status_path)
        raise StatusFileError("Valid status file not found")

    @abstractmethod
    def _status_exists(self):
        """Check if status file exists"""

    def get_full_copy_name(self, file_path):
        """
        For a given backup copy find a parent. If it's a full copy
        then return itself

        :param file_path:
        :return:
        """
        try:
            for run_type in INTERVALS:
                for key in self.status()[run_type].keys():
                    if file_path.endswith(key):
                        if self.status()[run_type][key]['type'] == "full":
                            return file_path
                        else:
                            remote_part = file_path.replace(key, '')
                            parent = self.status()[run_type][key]['parent']
                            result = "%s%s" % (remote_part, parent)
                            return result
        except (TypeError, KeyError) as err:
            LOG.error('Failed to find parent of %s', file_path)
            raise DestinationError(err)

        raise DestinationError('Failed to find parent of %s' % file_path)

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
        latest = None
        max_finish = 0
        for _, backups in cur_status.iteritems():
            for key, backup in backups.iteritems():
                try:
                    if backup['backup_finished'] >= max_finish:
                        max_finish = backup['backup_finished']
                        latest = key
                except KeyError:
                    pass
        if latest is None:
            filename = None
            for _, backups in cur_status.iteritems():
                for key, _ in backups.iteritems():
                    backup_name = key.rsplit('/', 1)[-1]
                    if backup_name > filename:
                        filename = backup_name
                        latest = key
        if latest is None:
            return None
        url = "{remote_path}/{filename}".format(
            remote_path=self.remote_path,
            filename=latest
        )
        return url

    @abstractmethod
    def _get_file_content(self, path):
        """Function for get file content by path"""

    def _is_valid_status(self, path):
        expected_status = json.loads(
            base64.b64decode(
                self._get_file_content(path)
            )
        )
        if "checksum" not in expected_status:
            LOG.debug("Checksum key not found in expected status")
            return False

        expected_md5 = expected_status.pop('checksum')
        actual_md5 = hashlib.md5(
            json.dumps(
                expected_status, sort_keys=True
            )
        ).hexdigest()
        return actual_md5 == expected_md5

    def _get_pretty_status(self, path):
        status = json.loads(
            base64.b64decode(
                self._get_file_content(path)
            )
        )
        del status['checksum']
        return status

    @abstractmethod
    def _move_file(self, source, destination):
        pass
