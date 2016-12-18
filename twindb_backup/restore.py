import ConfigParser
import base64
import json
import shlex
from subprocess import Popen, PIPE
import os
import tempfile
import errno
import psutil
from twindb_backup import log, INTERVALS
from twindb_backup.destination.base_destination import DestinationError
from twindb_backup.destination.local import Local
from twindb_backup.util import get_destination, mkdir_p, \
    get_hostname_from_backup_copy


def _get_status_key(status, key, variable):
    log.debug('status = %s' % json.dumps(status, indent=4, sort_keys=True))
    log.debug('key = %s' % key)
    try:
        for run_type in INTERVALS:
            if key in status[run_type]:
                return status[run_type][key][variable]
    except KeyError:
        pass
    log.warning('key %s is not found' % key)
    return None


def get_galera_version(status, key):
    return _get_status_key(status, key, 'wsrep_provider_version')


def get_backup_type(status, key):
    backup_type = _get_status_key(status, key, 'type')
    if backup_type:
        return backup_type

    raise DestinationError('Unknown backup type for backup copy %s' % key)


def get_my_cnf(status, key):
    return base64.b64decode(_get_status_key(status, key, 'config'))


def restore_from_mysql_full(dst, backup_copy, dst_dir, redo_only=False):
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
                          '--apply-log']
        if redo_only:
            xtrabackup_cmd += ['--redo-only']
        xtrabackup_cmd += [dst_dir]

        log.debug('Running %s' % ' '.join(xtrabackup_cmd))
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


def restore_from_mysql_incremental(dst, backup_copy, dst_dir):
    full_copy = dst.get_full_copy_name(backup_copy)
    restore_from_mysql_full(dst, full_copy, dst_dir, redo_only=True)
    inc_dir = tempfile.mkdtemp()
    try:
        with dst.get_stream(backup_copy) as handler:
            try:
                gunzip_cmd = "gunzip"
                log.debug('Running %s', gunzip_cmd)
                gunzip_proc = Popen(shlex.split(gunzip_cmd),
                                    stdin=handler,
                                    stdout=PIPE,
                                    stderr=PIPE,
                                    cwd=inc_dir)
                try:
                    xbstream_cmd = ['xbstream', '-x']
                    xbstream_proc = Popen(xbstream_cmd,
                                          stdin=gunzip_proc.stdout,
                                          stdout=PIPE,
                                          stderr=PIPE,
                                          cwd=inc_dir)
                    cout, cerr = xbstream_proc.communicate()
                    ret = xbstream_proc.returncode
                    if ret:
                        log.error('%r exited with code %d'
                                  % (xbstream_cmd, ret))
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
                              '--use-memory=%d' % (mem_usage.available / 2),
                              '--apply-log', '--redo-only', dst_dir,
                              '--incremental-dir', inc_dir]
            log.debug('Running %s' % ' '.join(xtrabackup_cmd))
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

            xtrabackup_cmd = ['innobackupex',
                              '--use-memory=%d' % (mem_usage.available / 2),
                              '--apply-log', dst_dir]
            log.debug('Running %s' % ' '.join(xtrabackup_cmd))
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
    finally:
        try:
            pass
        except OSError as exc:
            if exc.errno != errno.ENOENT:  # ENOENT - no such file or directory
                raise  # re-raise exception


def gen_grastate(path, version, uuid, seqno):
    with open(path, 'w') as fp:
        fp.write("""# GALERA saved state
version: {version}
uuid:    {uuid}
seqno:   {seqno}
cert_index:
""".format(version=version, uuid=uuid, seqno=seqno))


def restore_from_mysql(config, backup_copy, dst_dir):
    log.info('Restoring %s in %s' % (backup_copy, dst_dir))
    mkdir_p(dst_dir)

    try:
        keep_local_path = config.get('destination', 'keep_local_path')
        if os.path.exists(backup_copy) \
                and backup_copy.startswith(keep_local_path):
            dst = Local(keep_local_path)
        else:
            hostname = get_hostname_from_backup_copy(backup_copy)
            if not hostname:
                raise DestinationError('Failed to get hostname from %s'
                                       % backup_copy)
            dst = get_destination(config, hostname=hostname)
    except ConfigParser.NoOptionError:
        dst = get_destination(config)

    key = dst.basename(backup_copy)
    status = dst.status()

    if get_backup_type(status, key) == "full":
        restore_from_mysql_full(dst, backup_copy, dst_dir)
    else:
        restore_from_mysql_incremental(dst, backup_copy, dst_dir)

    my_cnf = get_my_cnf(status, key)
    if my_cnf:
        with open(dst_dir + '/my.cnf.orig', 'w') as fp:
            fp.write(my_cnf)

    if os.path.exists(dst_dir + '/xtrabackup_galera_info'):
        version = get_galera_version(status, key)

        with open(dst_dir + '/xtrabackup_galera_info') as fp:
            galera_info = fp.read()
            uuid = galera_info.split(':')[0]
            seqno = galera_info.split(':')[1]

        gen_grastate(dst_dir + '/grastate.dat',
                     version, uuid, seqno)

    log.info('Successfully restored %s in %s' % (backup_copy, dst_dir))
    log.info('Now copy content of %s to MySQL datadir: '
             'cp -R %s/* /var/lib/mysql/' % (dst_dir, dst_dir))
    log.info('Fix permissions: chown -R mysql:mysql /var/lib/mysql/')
    log.info('Make sure innodb_log_file_size and innodb_log_files_in_group '
             'in %s/backup-my.cnf and in /etc/my.cnf are same' % dst_dir)
    if my_cnf:
        log.info('Original my.cnf is restore in %s/my.cnf' % dst_dir)
    log.info('Then you can start MySQL normally')


def restore_from_file(config, backup_copy, dst_dir):
    log.info('Restoring %s in %s' % (backup_copy, dst_dir))
    mkdir_p(dst_dir)

    if os.path.exists(backup_copy):
        dst = Local(backup_copy)
    else:
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
