from contextlib import contextmanager
import os
import shlex
from subprocess import Popen, PIPE
import tempfile
from twindb_backup import log, get_files_to_delete
from twindb_backup.source.base_source import BaseSource


class MySQLSource(BaseSource):
    def __init__(self, defaults_file, run_type):
        self.defaults = defaults_file
        self._suffix = 'xbstream.gz'
        self._media_type = 'mysql'
        super(MySQLSource, self).__init__(run_type)

    @contextmanager
    def get_stream(self):
        """
        Get a PIPE handler with content of the source
        :return:
        """
        cmd = "innobackupex --defaults-file=%s --stream xbstream " \
              "--host 127.0.0.1 ." \
              % self.defaults
        try:
            log.debug('Running %s', cmd)
            stderr_file = tempfile.NamedTemporaryFile(delete=False)
            proc_innobackupex = Popen(shlex.split(cmd),
                                      stderr=stderr_file,
                                      stdout=PIPE)
            cmd = "gzip -c -"
            try:
                log.debug('Running %s', cmd)
                proc_gzip = Popen(shlex.split(cmd),
                                  stdin=proc_innobackupex.stdout,
                                  stderr=PIPE, stdout=PIPE)
                yield proc_gzip.stdout

                cout, cerr = proc_gzip.communicate()
                if proc_gzip.returncode:
                    log.error('Failed to compress innobackupex stream: '
                              '%s' % cerr)
                    exit(1)
                else:
                    log.debug('Successfully compressed innobackupex stream')

            except OSError as err:
                log.error('Failed to run %s: %s', cmd, err)
                exit(1)

            proc_innobackupex.communicate()
            if proc_innobackupex.returncode:
                log.error('Failed to run innobackupex. '
                          'Check error output in %s', stderr_file.name)
                exit(1)
            else:
                log.debug('Successfully streamed innobackupex output')
            log.debug('innobackupex error log file %s', stderr_file.name)
            os.unlink(stderr_file.name)

        except OSError as err:
            log.error('Failed to run %s: %s', cmd, err)
            exit(1)

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return self._get_name('mysql')

    def apply_retention_policy(self, dst, config, run_type):

        prefix = "{remote_path}/{prefix}/mysql/mysql-".format(
            remote_path=dst.remote_path,
            prefix=self.get_prefix()
        )
        keep_copies = config.getint('retention',
                                    '%s_copies' % run_type)

        objects = dst.list_files(prefix)

        for fl in get_files_to_delete(objects, keep_copies):
            log.debug('Deleting remote file %s' % fl)
            dst.delete(fl)

        self._delete_local_files('mysql', config)
