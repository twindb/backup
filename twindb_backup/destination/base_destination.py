import os
import errno
from abc import abstractmethod
from contextlib import contextmanager

from subprocess import Popen, PIPE

from twindb_backup import log


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

    @contextmanager
    def _get_input_handler(self, handler, keep_local, name):

        if keep_local:
            local_name = keep_local + '/' + name
            self._mkdir_p(os.path.dirname(local_name))
            tee_cmd = [
                'tee',
                local_name
            ]
            log.debug('Running %s', ' '.join(tee_cmd))
            proc_tee = Popen(tee_cmd, stdin=handler, stdout=PIPE, stderr=PIPE)

            yield proc_tee.stdout

            if proc_tee:
                cout, cerr = proc_tee.communicate()
                if proc_tee.returncode:
                    log.error('%s existed with error code %d',
                              ' '.join(tee_cmd), proc_tee.returncode)
                    if cout:
                        log.info(cout)
                    exit(1)
        else:
            yield handler

    def _save(self, cmd, handler, keep_local, name):
        with self._get_input_handler(handler, keep_local, name) \
                as input_handler:
            log.debug('Running %s', ' '.join(cmd))
            proc = Popen(cmd, stdin=input_handler, stdout=PIPE, stderr=PIPE)
            cout_ssh, cerr_ssh = proc.communicate()

            if proc.returncode:
                log.error('%s exited with error code %d',
                          ' '.join(cmd), proc.returncode)
                if cout_ssh:
                    log.info(cout_ssh)
                if cerr_ssh:
                    log.error(cerr_ssh)
                exit(1)

        return proc.returncode

    @abstractmethod
    def list_files(self, prefix):
        pass

    @abstractmethod
    def delete(self, obj):
        pass
