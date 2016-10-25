import socket


class BaseSource(object):
    """
    Base source for backup
    """
    run_type = None
    procs = []

    def __init__(self, run_type):
        self.run_type = run_type

    def get_stream(self):
        """
        Get backup stream in a handler
        """

    def get_prefix(self):
        return "{run_type}/{hostname}".format(
            run_type=self.run_type,
            hostname=socket.gethostname()
        )

    @staticmethod
    def _sanitize_filename(path):
        return path.rstrip('/').replace('/', '_')

    def get_procs(self):
        return self.procs

