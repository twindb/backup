import shlex
from contextlib import contextmanager
from subprocess import Popen, PIPE
from twindb_backup import LOG, get_files_to_delete
from twindb_backup.source.base_source import BaseSource


class FileSource(BaseSource):
    def __init__(self, path, run_type):
        self.path = path
        self._suffix = 'tar.gz'
        self._media_type = 'files'
        super(FileSource, self).__init__(run_type)

    @contextmanager
    def get_stream(self):
        """
        Get a PIPE handler with content of the source
        :return:
        """
        cmd = "tar zcf - %s" % self.path
        try:
            LOG.debug('Running %s', cmd)
            proc = Popen(shlex.split(cmd), stderr=PIPE, stdout=PIPE)
            self.procs.append(proc)

            yield proc.stdout

            cout, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('Failed to read from %s: %s' % (self.path, cerr))
                exit(1)
            else:
                LOG.debug('Successfully streamed %s', self.path)

        except OSError as err:
            LOG.error('Failed to run %s: %s', cmd, err)
            exit(1)

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return self._get_name(self._sanitize_filename())

    def _sanitize_filename(self):
        return self.path.rstrip('/').replace('/', '_')

    def apply_retention_policy(self, dst, config, run_type):
        if dst.remote_path:
            remote_path = dst.remote_path + '/'
        else:
            remote_path = ''
        prefix = "{remote_path}{prefix}/files/{file}".format(
            remote_path=remote_path,
            prefix=self.get_prefix(),
            file=self._sanitize_filename()
        )
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)

        backups_list = dst.list_files(prefix)
        LOG.debug('Remote copies: %r', backups_list)
        for fl in get_files_to_delete(backups_list, keep_copies):
            LOG.debug('Deleting remote file %s' % fl)
            dst.delete(fl)

        self._delete_local_files(self._sanitize_filename(), config)
