# -*- coding: utf-8 -*-
"""
Module defines Base destination class and destination exception(s).
"""
from abc import abstractmethod

from subprocess import Popen, PIPE

from twindb_backup import LOG, INTERVALS


class DestinationError(Exception):
    """General destination error"""
    pass


class BaseDestination(object):
    """Base destination class"""

    def __init__(self):
        self.remote_path = ''

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

        :param status: dictionary like

        ::

            {
                'hourly': [
                    {
                        'filename': '/remote/path',
                        'binlog': 'mysql-bin.000001',
                        'position': 43670
                    }
                ]
            }

        :return: dictionary with the status
        """
        if status:
            return self._write_status(status)
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

    @abstractmethod
    def share(self, url):
        """Share backup copy for public access"""
        raise NotImplementedError('Method share() not implemented for '
                                  'this backup destination')

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
            for _, backups in cur_status.iteritems():
                for key, _ in backups.iteritems():
                    backup_name = key.url.rsplit('/', 1)[-1]
                    if backup_name > latest:
                        latest = backup_name
        if latest is None:
            return None
        url = "{remote_path}/{filename}".format(remote_path=self.remote_path, filename=latest)
        return url
