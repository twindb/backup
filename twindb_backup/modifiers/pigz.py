# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with pigz
"""
from contextlib import contextmanager
from subprocess import Popen, PIPE
import psutil

from twindb_backup.modifiers.base import Modifier


class Pigz(Modifier):
    """
    Modifier that compresses the input_stream with pigz.
    """
    suffix = ".gz"

    def __init__(self, input_stream, threads=None, level=9):
        """
        Modifier that uses pigz compression

        :param input_stream: Input stream. Must be file object
        :param threads: number of threads to use (defaults to total-1)
        :type threads: int|string
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int|string
        """
        super(Pigz, self).__init__(input_stream)

        if threads is None or threads == '':
            threads = max(psutil.cpu_count() - 1, 1)

        if level is None or level == '':
            level = 9

        self._threads = int(threads)
        self._level = int(level)

    def get_compression_cmd(self):
        """get compression program cmd"""
        return ['pigz', '-{0}'.format(self._level), '-p', str(self._threads), '-c', '-']

    def get_decompression_cmd(self):
        """get decompression program cmd"""
        return ['pigz', '-p', str(self._threads), '-d', '-c']

    @contextmanager
    def get_stream(self):
        """
        Compress the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the pigz command
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
