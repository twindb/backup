# -*- coding: utf-8 -*-
"""
Module defines modifier that compresses a stream with bzip2
"""
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
        :type level: int
        """
        super(Bzip2, self).__init__(input_stream)

        self._level = level

    @property
    def _modifier_cmd(self):
        """get compression program cmd"""
        return ["bzip2", "-{0}".format(self._level), "-c", "-"]

    @property
    def _unmodifier_cmd(self):
        """get decompression program cmd"""
        return ["bunzip2", "-d", "-c"]
