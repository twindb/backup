import base64
from contextlib import contextmanager
import json
import os
import socket
from subprocess import Popen, PIPE
from twindb_backup import LOG
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
        return self._save(cmd, handler)

    def list_files(self, prefix, recursive=False):

        if recursive:
            ls_cmd = ["ls", "-R", "%s*" % prefix]
        else:
            ls_cmd = ["ls", "%s*" % prefix]

        cmd = ls_cmd
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, _ = proc.communicate()

        return sorted(cout.split())

    def find_files(self, prefix, run_type):

        cmd = ["find", "%s*" % prefix, "-type", "f"]
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, _ = proc.communicate()

        return sorted(cout.split())

    def delete(self, obj):
        cmd = ["rm", obj]
        LOG.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd)
        proc.communicate()

    @staticmethod
    @contextmanager
    def get_stream(path):
        """
        Get a PIPE handler with content of the backup copy streamed from
        the destination

        :return:
        """
        cmd = ["cat", path]
        try:
            LOG.debug('Running %s', " ".join(cmd))
            proc = Popen(cmd, stderr=PIPE, stdout=PIPE)

            yield proc.stdout

            _, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('Failed to read from %s: %s', path, cerr)
                exit(1)
            else:
                LOG.debug('Successfully streamed %s', path)

        except OSError as err:
            LOG.error('Failed to run %s: %s', cmd, err)
            exit(1)

    def _write_status(self, status):
        raw_status = base64.b64encode(json.dumps(status))
        with open(self.status_path, 'w') as fstatus:
            fstatus.write(raw_status)
        return status

    def _read_status(self):
        if not self._status_exists():
            return self._empty_status

        with open(self.status_path) as status_descriptor:
            cout = status_descriptor.read()
            return json.loads(base64.b64decode(cout))

    def _status_exists(self):
        return os.path.exists(self.status_path)
