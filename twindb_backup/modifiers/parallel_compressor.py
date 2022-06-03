# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with pigz
"""
from psutil import cpu_count

from twindb_backup.modifiers.base import Modifier

DEFAULT_THREADS = cpu_count() - 1


class ParallelCompressor(Modifier):
    """
    Modifier that compresses the input_stream with pigz.
    """

    def __init__(
        self,
        input_stream,
        program="pigz",
        threads=DEFAULT_THREADS,
        level=9,
        suffix=".gz",
    ):
        """
        Modifier that compresses in multiple threads.

        :param input_stream: Input stream. Must be file object
        :param threads: number of threads to use (defaults to total-1)
        :type threads: int
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int
        """
        super(ParallelCompressor, self).__init__(input_stream)

        self._threads = threads
        self._level = level
        self._program = program
        self._suffix = suffix

    @property
    def suffix(self):
        """File name suffix, specific for the compression tool"""
        return self._suffix

    @property
    def _modifier_cmd(self):
        """get compression program cmd"""
        return [
            self._program,
            "-{0}".format(self._level),
            "-p",
            str(self._threads),
            "-c",
            "-",
        ]

    @property
    def _unmodifier_cmd(self):
        """get decompression program cmd"""
        return [self._program, "-p", str(self._threads), "-d", "-c"]
