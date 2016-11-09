import shlex
from subprocess import Popen, PIPE
import psutil
from twindb_backup import log
from twindb_backup.util import get_destination, mkdir_p

__author__ = 'aleks'


def restore_from_mysql(config, backup_copy, dst_dir):
    log.info('Restoring %s in %s' % (backup_copy, dst_dir))
    mkdir_p(dst_dir)
    dst = get_destination(config)

    with dst.get_stream(backup_copy) as handler:
        try:
            gunzip_cmd = "gunzip"
            log.debug('Running %s', gunzip_cmd)
            gunzip_proc = Popen(shlex.split(gunzip_cmd),
                                stdin=handler,
                                stdout=PIPE,
                                stderr=PIPE,
                                cwd=dst_dir)
            try:
                xbstream_cmd = ['xbstream', '-x']
                xbstream_proc = Popen(xbstream_cmd,
                                      stdin=gunzip_proc.stdout,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      cwd=dst_dir)
                cout, cerr = xbstream_proc.communicate()
                ret = xbstream_proc.returncode
                if ret:
                    log.error('%r exited with code %d' % (xbstream_cmd, ret))
                    if cout:
                        log.error('STDOUT: %s' % cout)
                    if cerr:
                        log.error('STDERR: %s' % cerr)
                    exit(1)

            except OSError as err:
                log.error('Failed to unarchive %s: %s', backup_copy, err)
                exit(1)
            cout, cerr = gunzip_proc.communicate()
            ret = gunzip_proc.returncode
            if ret:
                log.error('%s exited with code %d' % (gunzip_cmd, ret))
                if cout:
                    log.error('STDOUT: %s' % cout)
                if cerr:
                    log.error('STDERR: %s' % cerr)
                exit(1)
        except OSError as err:
            log.error('Failed to decompress %s: %s', backup_copy, err)
            exit(1)

    mem_usage = psutil.virtual_memory()
    try:
        xtrabackup_cmd = ['innobackupex',
                          '--use-memory=%d' % (mem_usage.available/2),
                          '--apply-log',
                          dst_dir]
        xtrabackup_proc = Popen(xtrabackup_cmd,
                                stdout=None,
                                stderr=None)
        xtrabackup_proc.communicate()
        ret = xtrabackup_proc.returncode
        if ret:
            log.error('%s exited with code %d' %
                      (
                          " ".join(xtrabackup_cmd),
                          ret
                      ))
    except OSError as err:
        log.error('Failed to prepare backup in %s: %s', dst_dir, err)
        exit(1)

    log.info('Successfully restored %s in %s' % (backup_copy, dst_dir))
    log.info('Now copy content of %s to MySQL datadir: '
             'cp -R %s/* /var/lib/mysql/' % (dst_dir, dst_dir))
    log.info('Fix permissions: chown -R mysql:mysql /var/lib/mysql/')
    log.info('Make sure innodb_log_file_size and innodb_log_files_in_group '
             'in %s/backup-my.cnf and in /etc/my.cnf are same' % dst_dir)
    log.info('Then you can start MySQL normally')


def restore_from_file(config, backup_copy, dst_dir):
    log.info('Restoring %s in %s' % (backup_copy, dst_dir))
    mkdir_p(dst_dir)
    dst = get_destination(config)
    cmd = "tar zvxf -"
    with dst.get_stream(backup_copy) as handler:
        try:
            log.debug('Running %s', cmd)
            proc = Popen(shlex.split(cmd), stdin=handler, cwd=dst_dir)
            cout, cerr = proc.communicate()
            ret = proc.returncode
            if ret:
                log.error('%s exited with code %d' % (cmd, ret))
                if cout:
                    log.error('STDOUT: %s' % cout)
                if cerr:
                    log.error('STDERR: %s' % cerr)
                exit(1)
        except OSError as err:
            log.error('Failed to decompress %s: %s', backup_copy, err)
            exit(1)
    log.info('Successfully restored %s in %s' % (backup_copy, dst_dir))
