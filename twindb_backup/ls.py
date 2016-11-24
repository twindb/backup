from ConfigParser import NoOptionError
import os
from subprocess import Popen
from twindb_backup import log
from twindb_backup.backup import get_destination


def list_available_backups(config):
    try:
        keep_local_path = config.get('destination', 'keep_local_path')
        log.info('Local copies:')

        if os.path.exists(keep_local_path):
            cmd = ["find", keep_local_path, '-type', 'f']
            log.debug('Running %s', ' '.join(cmd))
            proc = Popen(cmd)
            proc.communicate()
    except NoOptionError:
        pass

    for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
        log.info('%s copies:', run_type)
        dst = get_destination(config)

        for copy in dst.find_files(dst.remote_path, run_type):
            print(copy)
