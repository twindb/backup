import base64
from contextlib import contextmanager
import json
import os
import socket
from subprocess import Popen, PIPE
from twindb_backup import log
from twindb_backup.destination.base_destination import BaseDestination


class Local(BaseDestination):
    def __init__(self, path=None):
        super(Local, self).__init__()
        self.path = path
        self.remote_path = self.path
        try:
            os.mkdir(self.path)
        except OSError as err:
            if err.errno == 17:  # OSError: [Errno 17] File exists
                pass
            else:
                raise
        self.status_path = "{path}/{hostname}/status".format(
            path=self.path,
            hostname=socket.gethostname()
        )

    def save(self, handler, name):
        """
        Read from handler and save it on local storage

        :param name: store backup copy as this name
        :param handler:
        :return: exit code
        """
        local_name = self.path + '/' + name
        cmd = ["cat", "-", local_name]
        return self._save(cmd, handler, None, name)

    def list_files(self, prefix, recursive=False):

        if recursive:
            ls_cmd = ["ls", "-R", "%s*" % prefix]
        else:
            ls_cmd = ["ls", "%s*" % prefix]

        cmd = ls_cmd
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        return sorted(cout.split())

    def find_files(self, prefix):

        cmd = ["find", "%s*" % prefix, "-type", "f"]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        return sorted(cout.split())

    def delete(self, obj):
        cmd = ["rm", obj]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd)
        proc.communicate()

    @contextmanager
    def get_stream(self, path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination
        :return:
        """
        cmd = ["cat", path]
        try:
            log.debug('Running %s', " ".join(cmd))
            proc = Popen(cmd, stderr=PIPE, stdout=PIPE)

            yield proc.stdout

            cout, cerr = proc.communicate()
            if proc.returncode:
                log.error('Failed to read from %s: %s' % (path, cerr))
                exit(1)
            else:
                log.debug('Successfully streamed %s', path)

        except OSError as err:
            log.error('Failed to run %s: %s', cmd, err)
            exit(1)

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))
        with open(self.status_path, 'w') as fstatus:
            fstatus.write(raw_status)
        return status

    def _read_status(self):
        if not self._status_exists():
            return self._empty_status

        with open(self.status_path) as fp:
            cout = fp.read()
            return json.loads(base64.b64decode(cout))

    def _status_exists(self):
        return os.path.exists(self.status_path)
