# -*- coding: utf-8 -*-
"""
Module defines modifier that save a stream on the local file system
"""
import os

from twindb_backup.destination.local import Local
from twindb_backup.modifiers.base import Modifier, ModifierException
from twindb_backup.status.mysql_status import MySQLStatus
from twindb_backup.util import mkdir_p


class KeepLocal(Modifier):
    """KeepLocal() class saves a copy of the stream on the local file system.
    It doesn't alter the stream."""

    def __init__(self, input_stream, local_path):
        """
        Modifier that saves a local copy of the stream in local_path file.

        :param input_stream: Input stream. Must be file object
        :param local_path: path to local file
        """
        super(KeepLocal, self).__init__(input_stream)
        self.local_path = local_path
        local_dir = os.path.dirname(self.local_path)
        try:
            mkdir_p(local_dir)
        except OSError as err:
            raise ModifierException("Failed to create directory %s: %s" % (local_dir, err))

    def callback(self, **kwargs):
        local_dst = Local(kwargs["keep_local_path"])
        status = MySQLStatus(dst=kwargs["dst"])
        status.save(local_dst)

    @property
    def _modifier_cmd(self):
        """get compression program cmd"""
        return ["tee", self.local_path]
