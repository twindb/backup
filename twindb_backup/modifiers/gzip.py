# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with gzip
"""
from twindb_backup.modifiers.base import Modifier


class Gzip(Modifier):
    """
    Modifier that compresses the input_stream with gzip.
    """

    suffix = ".gz"

    def __init__(self, input_stream, level=9):
        """
        Modifier that uses gzip compression

        :param input_stream: Input stream. Must be file object
        :param level: compression level from 1 to 9 (fastest to best)
        :type level: int
        """
        super(Gzip, self).__init__(input_stream)

        self._level = level

    @property
    def _modifier_cmd(self):
        """get compression program cmd"""
        return ["gzip", "-{0}".format(self._level), "-c", "-"]

    @property
    def _unmodifier_cmd(self):
        """get decompression program cmd"""
        return ["gunzip", "-c"]
