from contextlib import contextmanager


class ModifierException(Exception):
    """Base Exception for Modifier error"""


class Modifier(object):
    def __init__(self, input_stream, callback=None,
                 **callback_kwargs):
        """
        Base Modifier class that takes input stream, modifies it somehow
        and returns output stream.
        After the input stream comes to the end a callback function is called

        :param input_stream: Input stream handle. It's like returned by proc.stdout
        :param callback: callback function that is called after
            the input stream ends
        :param **callback_kwargs: dictionary with callback function arguments
        """
        self.input = input_stream
        self.callback = callback
        self.callback_kwargs = callback_kwargs

    @contextmanager
    def get_stream(self):
        """
        Apply modifier and return output stream.
        The Base modifier does nothing, so it will return the input stream
        without modifications

        :return: output stream handle
        """
        yield self.input
        self._call_calback()

    def _call_calback(self):
        if self.callback:
            self.callback(**self.callback_kwargs)


