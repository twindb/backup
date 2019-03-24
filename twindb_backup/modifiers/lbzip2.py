# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with lbzip2
"""
from contextlib import contextmanager
from subprocess import Popen, PIPE
import psutil

from twindb_backup.modifiers.base import Modifier


class Lbzip2(Modifier):
    """
    Modifier that compresses the input_stream with lbzip2.
    """
    suffix = ".bz"

    def __init__(self, input_stream, threads=None, level=9):
        """
        Modifier that uses lbzip2 compression

        :param input_stream: Input stream. Must be file object
        :param threads: number of threads to use (defaults to total-1)
        :type threads: int|string
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int|string
        """
        super(Lbzip2, self).__init__(input_stream)

        if threads is None or threads == '':
            threads = max(psutil.cpu_count() - 1, 1)

        if level is None or level == '':
            level = 9

        self._threads = int(threads)
        self._level = int(level)

    def get_compression_cmd(self):
        """get compression program cmd"""
        return ['lbzip2', '-{0}'.format(self._level), '-n', str(self._threads), '-c', '-']

    def get_decompression_cmd(self):
        """get decompression program cmd"""
        return ['lbzip2', '-n', str(self._threads), '-d', '-c']

    @contextmanager
    def get_stream(self):
        """
        Compress the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the lbzip2 command
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
