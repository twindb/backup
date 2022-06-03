# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with pigz
"""
from psutil import cpu_count

from twindb_backup.modifiers.parallel_compressor import ParallelCompressor

DEFAULT_THREADS = cpu_count() - 1


class Pigz(ParallelCompressor):
    """
    Modifier that compresses the input_stream with pigz.
    """

    def __init__(self, input_stream, threads=DEFAULT_THREADS, level=9):
        """
        Modifier that uses pigz compression

        :param input_stream: Input stream. Must be file object
        :param threads: number of threads to use (defaults to total-1)
        :type threads: int
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int
        """
        super(Pigz, self).__init__(
            input_stream,
            program="pigz",
            threads=threads,
            level=level,
            suffix=".gz",
        )
