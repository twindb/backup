# -*- coding: utf-8 -*-
"""
Module defines Modifier() base class and its errors.
"""
from contextlib import contextmanager

from subprocess import Popen, PIPE

from twindb_backup import LOG


class ModifierException(Exception):
    """Base Exception for Modifier error"""


class Modifier(object):
    """Base Modifier class"""
    def __init__(self, input_stream):
        """
        Base Modifier class that takes input stream, modifies it somehow
        and returns output stream.
        After the input stream comes to the end a callback function is called

        :param input_stream: Input stream handle.
            It's like returned by proc.stdout
        """
        self.input = input_stream

    @contextmanager
    def get_stream(self):
        """
        Apply modifier and return output stream.
        The Base modifier does nothing, so it will return the input stream
        without modifications

        :return: output stream handle
        """
        yield self.input

    @contextmanager
    def _revert_stream(self, cmd):
        """
        Un-Apply modifier and return output stream.
        The Base modifier does nothing, so it will return the input stream
        without modifications

        :return: output stream handle
        """
        with self.input as input_stream:
            LOG.debug('Running %s', ' '.join(cmd))
            proc = Popen(cmd,
                         stdin=PIPE,
                         stdout=PIPE,
                         stderr=PIPE)
            proc.stdin.write(input_stream.read())
            proc.stdin.close()
            yield proc.stdout

            _, cerr = proc.communicate()
            if proc.returncode:
                LOG.error('%s exited with non-zero code.', ' '.join(cmd))
                LOG.error(cerr)

    def callback(self, **kwargs):
        """Method that will be called after the stream ends"""
        pass
