import ConfigParser
import errno
import os
import pymysql
import socket

from contextlib import contextmanager
from pymysql.cursors import DictCursor
from twindb_backup import log, INTERVALS
from twindb_backup.destination.s3 import S3
from twindb_backup.destination.ssh import Ssh


def get_destination(config, hostname=socket.gethostname()):
    destination = None
    try:
        destination = config.get('destination', 'backup_destination')
        log.debug('Destination in the config %s', destination)
        destination = destination.strip('"\'')
    except ConfigParser.NoOptionError:
        log.critical("Backup destination must be specified "
                     "in the config file")
        exit(-1)

    if destination == "ssh":
        host = config.get('ssh', 'backup_host')
        try:
            port = config.get('ssh', 'port')
        except ConfigParser.NoOptionError:
            port = 22
        try:
            ssh_key = config.get('ssh', 'ssh_key')
        except ConfigParser.NoOptionError:
            ssh_key = '/root/.ssh/id_rsa'
        user = config.get('ssh', 'ssh_user')
        remote_path = config.get('ssh', 'backup_dir')
        return Ssh(host=host, port=port, user=user, remote_path=remote_path,
                   key=ssh_key, hostname=hostname)

    elif destination == "s3":
        bucket = config.get('s3', 'BUCKET').strip('"\'')
        access_key_id = config.get('s3', 'AWS_ACCESS_KEY_ID').strip('"\'')
        secret_access_key = config.get('s3',
                                       'AWS_SECRET_ACCESS_KEY').strip('"\'')
        default_region = config.get('s3', 'AWS_DEFAULT_REGION').strip('"\'')
        return S3(bucket, access_key_id, secret_access_key,
                  default_region=default_region, hostname=hostname)

    else:
        log.critical('Destination %s is not supported' % destination)
        exit(-1)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_hostname_from_backup_copy(backup_copy):
    chunks = backup_copy.split('/')
    for run_type in INTERVALS:
        if run_type in chunks:
            return chunks[chunks.index(run_type) - 1]
    return None


@contextmanager
def get_connection(host=None, port=None, user=None, password=None,
                   defaults_file=None, mysql_sock=None, connect_timeout=10):
    db = None
    try:
        if defaults_file:
            db = pymysql.connect(
                read_default_file=defaults_file,
                connect_timeout=connect_timeout
            )
        elif mysql_sock:
            db = pymysql.connect(
                unix_socket=mysql_sock,
                user=user,
                passwd=password,
                cursorclass=DictCursor
            )
        elif port:
            db = pymysql.connect(
                host=host,
                port=port,
                user=user,
                passwd=password,
                cursorclass=DictCursor
            )
        else:
            db = pymysql.connect(
                host=host,
                user=user,
                passwd=password,
                cursorclass=DictCursor
            )

        yield db
    finally:
        if db:
            db.close()
