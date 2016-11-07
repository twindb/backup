from twindb_backup import log
from twindb_backup.backup import get_destination


def list_available_backups(config):
    for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
        log.info('%s copies:', run_type)
        dst = get_destination(config)

        if dst.remote_path:
            remote_path = dst.remote_path + '/'
        else:
            remote_path = ''

        prefix = "{remote_path}{prefix}/".format(
            remote_path=remote_path,
            prefix=run_type
        )
        for copy in dst.find_files(prefix):
            print(copy)
