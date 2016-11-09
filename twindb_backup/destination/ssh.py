from contextlib import contextmanager
import os
from subprocess import Popen, PIPE
from twindb_backup import log
from twindb_backup.destination.base_destination import BaseDestination


class Ssh(BaseDestination):
    def __init__(self, host='127.0.0.1', port=22, key='/root/.id_rsa',
                 user='root',
                 remote_path=None):
        super(Ssh, self).__init__()
        self.remote_path = remote_path
        self.user = user
        self.key = key
        self.port = port
        self.host = host
        self._ssh_command = ['ssh', '-l', self.user,
                             '-o',
                             'StrictHostKeyChecking=no',
                             '-o',
                             'PasswordAuthentication=no',
                             '-p', str(self.port),
                             '-i', key,
                             self.host]

    def save(self, handler, name, keep_local=None):
        """
        Read from handler and save it on remote ssh server

        :param name: store backup copy as this name
        :param keep_local: path to directory where to store a local kopy
        :param handler:
        :return: exit code
        """
        remote_name = self.remote_path + '/' + name
        self._mkdir_r(os.path.dirname(remote_name))
        cmd = self._ssh_command + ["cat - > \"%s\"" % remote_name]
        return self._save(cmd, handler, keep_local, name)

    def _mkdir_r(self, path):
        """
        Create directory on the remote server

        :param path: remote directory
        :return: exit code
        """
        cmd = self._ssh_command + ["mkdir -p \"%s\"" % path]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        if proc.returncode:
            log.error('Failed to create directory %s: %s' % (path, cerr))
            exit(1)

        return proc.returncode

    def list_files(self, prefix, recursive=False):

        if recursive:
            ls_cmd = ["ls -R %s*" % prefix]
        else:
            ls_cmd = ["ls %s*" % prefix]

        cmd = self._ssh_command + ls_cmd
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        return sorted(cout.split())

    def find_files(self, prefix):

        cmd = self._ssh_command + ["find %s*" % prefix]
        log.debug('Running %s', ' '.join(cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        cout, cerr = proc.communicate()

        return sorted(cout.split())

    def delete(self, obj):
        cmd = self._ssh_command + ["rm %s" % obj]
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
        cmd = self._ssh_command + ["cat %s" % path]
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
