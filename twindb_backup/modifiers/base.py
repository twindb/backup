# -*- coding: utf-8 -*-
"""
Module defines Modifier() base class and its errors.
"""
from contextlib import contextmanager
from subprocess import PIPE, Popen

from twindb_backup import LOG
from twindb_backup.modifiers.exceptions import ModifierException


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
        self._input = input_stream

    @property
    def input(self):
        """
        :return: Input stream to be modified
        """
        return self._input

    @contextmanager
    def get_stream(self):
        """
        Compress the input stream and return it as the output stream

        :return: output stream handle
        """
        with self._input as input_stream:
            LOG.debug("Running %s", " ".join(self._modifier_cmd))
            proc = Popen(
                self._modifier_cmd, stdin=input_stream, stdout=PIPE, stderr=PIPE
            )
            yield proc.stdout
            proc.communicate()

    @contextmanager
    def revert_stream(self):
        """
        Un-Apply modifier and return output stream.
        The Base modifier does nothing, so it will return the input stream
        without modifications

        :return: output stream handle
        """
        with self._input as input_stream:
            LOG.debug("Running %s", " ".join(self._unmodifier_cmd))
            proc = Popen(
                self._unmodifier_cmd,
                stdin=input_stream,
                stdout=PIPE,
                stderr=PIPE,
            )
            yield proc.stdout

            _, cerr = proc.communicate()
            if proc.returncode:
                msg = "%s exited with non-zero code." % " ".join(
                    self._unmodifier_cmd
                )
                LOG.error(msg)
                LOG.error(cerr)
                raise ModifierException(msg)

    def callback(self, **kwargs):
        """Method that will be called after the stream ends"""
        pass

    @property
    def _modifier_cmd(self):
        """
        Command that accepts a stream as STDIN, modifies it and returns result
        as STDOUT.

        :return: Modifier command
        :rtype: list
        """
        return ["cat", "-"]

    @property
    def _unmodifier_cmd(self):
        """
        Command that accepts a stream as STDIN, reverts the stream
        it and returns result as STDOUT.

        :return: Modifier command
        :rtype: list
        """
        return ["cat", "-"]
