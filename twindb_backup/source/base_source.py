import ConfigParser
import socket

import time

from twindb_backup import delete_local_files


class BaseSource(object):
    """
    Base source for backup
    """
    run_type = None
    procs = []
    _suffix = None
    _media_type = None

    def __init__(self, run_type):
        self.run_type = run_type

    def get_stream(self):
        """
        Get backup stream in a handler
        """

    def get_prefix(self):
        return "{run_type}/{hostname}".format(
            run_type=self.run_type,
            hostname=socket.gethostname()
        )

    def get_procs(self):
        return self.procs

    def _get_name(self, filename):

        return "{prefix}/{media_type}/{file}-{time}.{suffix}".format(
            prefix=self.get_prefix(),
            media_type=self._media_type,
            file=filename,
            time=time.strftime('%Y-%m-%d_%H_%M_%S'),
            suffix=self._suffix
        )

    def _delete_local_files(self, filename, config):
        try:
            keep_copies = config.getint('retention_local',
                                        '%s_copies' % self.run_type)
            keep_local_path = config.get('destination', 'keep_local_path')
            dir_backups = "{local_path}/{prefix}/{media_type}/{file}*".format(
                local_path=keep_local_path,
                prefix=self.get_prefix(),
                media_type=self._media_type,
                file=filename
            )
            delete_local_files(dir_backups, keep_copies)

        except ConfigParser.NoOptionError:
            pass
