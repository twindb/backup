# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with gzip
"""
from contextlib import contextmanager
from subprocess import Popen, PIPE

from twindb_backup.modifiers.base import Modifier


class Gzip(Modifier):
    def __init__(self, input_stream):
        """
        Modifier that compresses the input_stream with gzip.

        :param input_stream: Input stream. Must be file object
        """
        super(Gzip, self).__init__(input_stream)

    @contextmanager
    def get_stream(self):
        """
        Compress the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the gzip command
        """
        with self.input as input_stream:
            proc = Popen(['gzip', '-c', '-'],
                         stdin=input_stream,
                         stdout=PIPE)
            yield proc.stdout
            proc.communicate()
