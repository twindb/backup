import shlex
from subprocess import Popen, PIPE
import time
from twindb_backup import log
from twindb_backup.source.base_source import BaseSource


class FileSource(BaseSource):
    def __init__(self, path, run_type):
        self.path = path
        super(FileSource, self).__init__(run_type)

    def get_stream(self):
        """
        Get a PIPE handler with content of the source
        :return:
        """
        cmd = "tar zcf - %s" % self.path
        log.debug('Running %s', cmd)
        proc = Popen(shlex.split(cmd), stderr=PIPE, stdout=PIPE)
        self.procs.append(proc)
        return proc.stdout

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return "{prefix}/files/{file}-{time}.tar.gz".format(
            prefix=self.get_prefix(),
            file=self.sanitize_filename(),
            time=time.strftime('%Y-%m-%d_%H_%M_%S')
        )

    def sanitize_filename(self):
        return self.path.rstrip('/').replace('/', '_')
