from contextlib import contextmanager

from subprocess import Popen, PIPE

from twindb_backup.modifiers.base import Modifier


class KeepLocal(Modifier):
    def __init__(self, input_stream, local_path, callback=None,
                 **callback_kwargs):
        """
        Modifier that saves a local copy of the stream in local_path file.

        :param input_stream: Input stream. Must be file object
        :param local_path: path to local file
        :param callback: function to call after the input stream is over
        :param callback_kwargs: arguments for the callback function
        """
        super(KeepLocal, self).__init__(input_stream,
                                        callback, **callback_kwargs)
        self.local_path = local_path

    @contextmanager
    def get_stream(self):
        """
        Save a copy of the input stream and return the output stream

        :return: output stream handle
        :raise: OSError if failed to call the tee command
        """
        with self.input as input_stream:
            proc = Popen(['tee', self.local_path],
                         stdin=input_stream,
                         stdout=PIPE)
            yield proc.stdout
            proc.communicate()
            super(KeepLocal, self)._call_calback()
