# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with lbzip2
"""
from psutil import cpu_count

from twindb_backup.modifiers.parallel_compressor import ParallelCompressor

DEFAULT_THREADS = cpu_count() - 1


class Lbzip2(ParallelCompressor):
    """
    Modifier that compresses the input_stream with lbzip2.
    """

    def __init__(self, input_stream, threads=DEFAULT_THREADS, level=9):
        """
        Modifier that uses lbzip2 compression

        :param input_stream: Input stream. Must be file object
        :param threads: number of threads to use (defaults to total-1)
        :type threads: int
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int
        """
        super(Lbzip2, self).__init__(
            input_stream,
            program="lbzip2",
            threads=threads,
            level=level,
            suffix=".bz",
        )

    @property
    def _modifier_cmd(self):
        """get compression program cmd"""
        return [
            self._program,
            "-{0}".format(self._level),
            "-n",
            str(self._threads),
            "-c",
            "-",
        ]

    @property
    def _unmodifier_cmd(self):
        """get decompression program cmd"""
        return [self._program, "-n", str(self._threads), "-d", "-c"]
