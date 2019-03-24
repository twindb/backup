"""Compression configuration"""


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
        self._program = kwargs.get('program')
        self._threads = kwargs.get('threads', None)
        self._level = kwargs.get('level', None)

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
