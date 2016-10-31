import os
import errno


class DestinationError(Exception):
    pass


class BaseDestination(object):
    def __init__(self):
        pass

    def save(self, handler, name):
        pass

    @staticmethod
    def _mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def apply_retention_policy(self, config):

        for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
            self._apply_policy_by_type(config, run_type)

    def _apply_policy_by_type(self, config, run_type):
        self._apply_policy_for_files(config, run_type)
        self._apply_policy_for_mysql(config, run_type)

    def _apply_policy_for_files(self, config, run_type):
        raise DestinationError('Not implemented')

    def _apply_policy_for_mysql(self, config, run_type):
        raise DestinationError('Not implemented')


