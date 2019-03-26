"""Compression configuration"""
from twindb_backup.configuration.exceptions import ConfigurationError

from twindb_backup.modifiers import COMPRESSION_MODIFIERS


class CompressionConfig(object):
    """Compression configuration

    :param program: compression program
    :type program: str
    :param threads: number of threads
    :type threads: int
    :param level: compression level
    :type level: int
    """
    def __init__(self, **kwargs):
        self._program = kwargs.get('program', COMPRESSION_MODIFIERS.keys()[0])
        if self.program not in COMPRESSION_MODIFIERS:
            raise ConfigurationError(
                'Unsupported compression tool %s' % self.program
            )

        self._threads = int(kwargs.get('threads')) \
            if 'threads' in kwargs else None

        self._level = int(kwargs.get('level')) if 'level' in kwargs else None

    @property
    def program(self):
        """Compression program."""

        return self._program

    @property
    def threads(self):
        """Number of threads to use."""

        return self._threads

    @property
    def level(self):
        """Compression level."""

        return self._level

    def get_modifier(self, stream):
        """
        Build a compression modifier based on the given configuration

        :param stream: stream to compress
        :return: compression modifier
        :rtype: Modifier
        """
        kwargs = {
            key: getattr(self, key)
            for key in COMPRESSION_MODIFIERS[self.program]['kwargs']
            if getattr(self, key) is not None
        }

        return COMPRESSION_MODIFIERS[self.program]['class'](stream, **kwargs)
