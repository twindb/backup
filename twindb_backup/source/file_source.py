# -*- coding: utf-8 -*-
"""
Module defines File source class for backing up local directories.
"""
import shlex
from contextlib import contextmanager
from subprocess import Popen, PIPE
from twindb_backup import LOG, get_files_to_delete
from twindb_backup.source.base_source import BaseSource


class FileSource(BaseSource):
    """FileSource class"""

    def __init__(self, path, run_type):
        self.path = path
        self._suffix = 'tar'
        self._media_type = 'files'
        super(FileSource, self).__init__(run_type)

    @property
    def media_type(self):
        """Get media type. Media type is a general term that describes
        what you backup. For directories media_type is 'file'.

        :return: 'file'
        """
        return self._media_type

    @contextmanager
    def get_stream(self):
        """
        Get a PIPE handler with content of the source

        :return:
        """
        cmd = "tar cf - %s" % self.path
        try:
            LOG.debug('Running %s', cmd)
            proc = Popen(shlex.split(cmd), stderr=PIPE, stdout=PIPE)

            yield proc.stdout

            _, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('Failed to read from %s: %s', self.path, cerr)
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
        """Apply retention policy
        """
        prefix = "{remote_path}/{prefix}/files/{file}".format(
            remote_path=dst.remote_path,
            prefix=self.get_prefix(),
            file=self._sanitize_filename()
        )
        keep_copies = getattr(config.retention, run_type)

        backups_list = dst.list_files(prefix)

        LOG.debug('Remote copies: %r', backups_list)
        for backup_copy in get_files_to_delete(backups_list, keep_copies):
            LOG.debug('Deleting remote file %s', backup_copy)
            dst.delete(backup_copy)

        self._delete_local_files(self._sanitize_filename(), config)
