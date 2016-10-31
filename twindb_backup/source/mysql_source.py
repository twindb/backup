import shlex
from subprocess import Popen, PIPE
import time
from twindb_backup import log
from twindb_backup.source.base_source import BaseSource


class MySQLSource(BaseSource):
    def __init__(self, defaults_file, run_type):
        self.defaults = defaults_file
        super(MySQLSource, self).__init__(run_type)

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
            proc_innobackupex = Popen(shlex.split(cmd),
                                      stderr=PIPE,
                                      stdout=PIPE)
            self.procs.append(proc_innobackupex)
        except OSError as err:
            log.error('Failed to run %s: %s', cmd, err)
            raise

        cmd = "gzip -c -"
        try:
            log.debug('Running %s', cmd)
            proc_gzip = Popen(shlex.split(cmd), stdin=proc_innobackupex.stdout,
                              stderr=PIPE, stdout=PIPE)
            self.procs.append(proc_gzip)
        except OSError as err:
            log.error('Failed to run %s: %s', cmd, err)
            raise

        return proc_gzip.stdout

    def get_name(self, ):
        """
        Generate relative destination file name

        :return: file name
        """
        return "{prefix}/mysql/mysql-{time}.xbstream.gz".format(
            prefix=self.get_prefix(),
            time=time.strftime('%Y-%m-%d_%H_%M_%S')
        )
