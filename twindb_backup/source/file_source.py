# -*- coding: utf-8 -*-
"""
Module defines File source class for backing up local directories.
"""
import shlex
from contextlib import contextmanager
from subprocess import PIPE, Popen

from twindb_backup import LOG, get_files_to_delete
from twindb_backup.source.base_source import BaseSource


class FileSource(BaseSource):
    """
    FileSource class describes a local directory or file.
    The get_stream() method will return a compressed content of it.

    :param path: Path to local file or directory.
    :type path: str
    :param run_type: A string "daily", "weekly", etc.
    :type run_type: str
    :param tar_options: Additional options passed to ``tar``.
    :type tar_options: str
    """

    def __init__(self, path, run_type, tar_options: str = None):
        self.path = path
        self._suffix = "tar"
        self._media_type = "files"
        self._tar_options = tar_options
        super(FileSource, self).__init__(run_type)

    @property
    def media_type(self):
        """Get media type. Media type is a general term that describes
        what you back up. For directories media_type is 'file'.

        :return: 'file'
        :rtype: str
        """
        return self._media_type

    @contextmanager
    def get_stream(self):
        """
        Get a PIPE handler with content of the source

        :return:
        """
        cmd = ["tar", "cf", "-"]
        if self._tar_options:
            cmd.append(self._tar_options)
        cmd.append(self.path)
        try:
            LOG.debug("Running %s", " ".join(cmd))
            proc = Popen(cmd, stderr=PIPE, stdout=PIPE)

            yield proc.stdout

            _, cerr = proc.communicate()
            if proc.returncode:
                LOG.error("Failed to read from %s: %s", self.path, cerr)
                exit(1)
            else:
                LOG.debug("Successfully streamed %s", self.path)

        except OSError as err:
            LOG.error("Failed to run %s: %s", cmd, err)
            exit(1)

    def get_name(self):
        """
        Generate relative destination file name

        :return: file name
        """
        return self._get_name(self._sanitize_filename())

    def _sanitize_filename(self):
        return self.path.rstrip("/").replace("/", "_")

    def apply_retention_policy(self, dst, config, run_type):
        """Apply retention policy"""
        prefix = "{remote_path}/{prefix}/files/{file}".format(
            remote_path=dst.remote_path,
            prefix=self.get_prefix(),
            file=self._sanitize_filename(),
        )
        keep_copies = getattr(config.retention, run_type)

        backups_list = dst.list_files(prefix)

        LOG.debug("Remote copies: %r", backups_list)
        for backup_copy in get_files_to_delete(backups_list, keep_copies):
            LOG.debug("Deleting remote file %s", backup_copy)
            dst.delete(backup_copy)

        self._delete_local_files(self._sanitize_filename(), config)
