from ConfigParser import NoOptionError
import socket
from subprocess import Popen
from twindb_backup import log
from twindb_backup.backup import get_destination


def list_available_backups(config):
    try:
        keep_local_path = config.get('destination', 'keep_local_path')

        cmd = ["find", keep_local_path, '-type', 'f']
        log.debug('Running %s', ' '.join(cmd))
        log.info('Local copies')
        proc = Popen(cmd)
        proc.communicate()
    except NoOptionError:
        pass

    for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
        log.info('%s copies:', run_type)
        dst = get_destination(config)

        if dst.remote_path:
            remote_path = dst.remote_path + '/'
        else:
            remote_path = ''

        prefix = "{remote_path}{hostname}/{run_type}".format(
            remote_path=remote_path,
            hostname=socket.gethostname(),
            run_type=run_type
        )
        for copy in dst.find_files(prefix):
            print(copy)
