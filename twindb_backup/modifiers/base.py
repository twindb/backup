from contextlib import contextmanager


class ModifierException(Exception):
    """Base Exception for Modifier error"""


class Modifier(object):
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

    def callback(self, **kwargs):
        pass
