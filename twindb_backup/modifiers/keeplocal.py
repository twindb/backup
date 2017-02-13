# -*- coding: utf-8 -*-
"""
Module defines modifier that save a stream on the local file system
"""
import os
from contextlib import contextmanager

from subprocess import Popen, PIPE

from twindb_backup.destination.local import Local
from twindb_backup.modifiers.base import Modifier
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
        mkdir_p(local_dir)

    @contextmanager
    def get_stream(self):
        """
        Save a copy of the input stream and return the output stream

        :return: output stream handle
        :raise: OSError if failed to call the tee command
        """
        with self.input as input_stream:
            proc = Popen(['tee', self.local_path],
                         stdin=input_stream,
                         stdout=PIPE)
            yield proc.stdout
            proc.communicate()

    def callback(self, **kwargs):
        local_dst = Local(kwargs['keep_local_path'])
        local_dst.status(kwargs['dst'].status())
