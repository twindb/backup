# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with bzip2
"""
from contextlib import contextmanager
from subprocess import Popen, PIPE

from twindb_backup.modifiers.base import Modifier


class Bzip2(Modifier):
    """
    Modifier that compresses the input_stream with bzip2.
    """
    suffix = ".bz"

    def __init__(self, input_stream, level=9):
        """
        Modifier that uses bzip2 compression

        :param input_stream: Input stream. Must be file object
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int|string
        """
        super(Bzip2, self).__init__(input_stream)

        if level is None or level == '':
            level = 9

        self._level = int(level)

    def get_compression_cmd(self):
        """get compression program cmd"""
        return ['bzip2', '-{0}'.format(self._level), '-c', '-']

    def get_decompression_cmd(self):
        """get decompression program cmd"""
        return ['bunzip2', '-d', '-c']

    @contextmanager
    def get_stream(self):
        """
        Compress the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the bzip2 command
        """
        with self.input as input_stream:
            proc = Popen(self.get_compression_cmd(),
                         stdin=input_stream,
                         stdout=PIPE)
            yield proc.stdout
            proc.communicate()

    def revert_stream(self):
        """
        Decompress the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the gpg command
        """
        return self._revert_stream(self.get_decompression_cmd())
