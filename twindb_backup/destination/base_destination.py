import os
import errno
from abc import abstractmethod
from contextlib import contextmanager

from subprocess import Popen, PIPE

from twindb_backup import log, INTERVALS


class DestinationError(Exception):
    pass


class BaseDestination(object):
    def __init__(self):
        self.remote_path = ''

    @abstractmethod
    def save(self, handler, name):
        pass

    @staticmethod
    def _mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    @staticmethod
    def _save(cmd, handler):

        with handler as input_handler:
            log.debug('Running %s', ' '.join(cmd))
            try:
                proc = Popen(cmd, stdin=input_handler,
                             stdout=PIPE,
                             stderr=PIPE)
                cout_ssh, cerr_ssh = proc.communicate()

                ret = proc.returncode
                if ret:
                    log.error('%s exited with error code %d',
                              ' '.join(cmd), ret)
                    if cout_ssh:
                        log.info(cout_ssh)
                    if cerr_ssh:
                        log.error(cerr_ssh)
                    exit(1)
                log.debug('Exited with code %d' % ret)
                return ret
            except OSError as err:
                log.error('Failed to run %s: %s', ' '.join(cmd), err)
                exit(1)

    @abstractmethod
    def list_files(self, prefix, recursive=False):
        pass

    @abstractmethod
    def find_files(self, prefix, run_type):
        pass

    @abstractmethod
    def delete(self, obj):
        pass

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
        pass

    @abstractmethod
    def _read_status(self):
        pass

    @abstractmethod
    def _status_exists(self):
        pass

    def get_full_copy_name(self, file_path):
        remote_path = self.remote_path.rstrip('/')
        log.debug('remote_path = %s' % remote_path)
        key = file_path.replace(remote_path + '/', '', 1)
        for run_type in INTERVALS:
            if key in self.status()[run_type]:
                parent = self.status()[run_type][key]['parent']
                return "%s/%s" % (self.remote_path, parent)

        raise DestinationError('Failed to find parent of %s' % file_path)

    def basename(self, filename):
        return filename.replace(self.remote_path + '/', '', 1)
