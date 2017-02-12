# -*- coding: utf-8 -*-
"""
Module that restores backup copies.
"""
from __future__ import print_function
import ConfigParser
import base64
import json
import shlex
from subprocess import Popen, PIPE
import os
import tempfile
import errno
import psutil
from twindb_backup import LOG, INTERVALS
from twindb_backup.destination.base_destination import DestinationError
from twindb_backup.destination.local import Local
from twindb_backup.util import get_destination, mkdir_p, \
    get_hostname_from_backup_copy


def _get_status_key(status, key, variable):
    LOG.debug('status = %s', json.dumps(status, indent=4, sort_keys=True))
    LOG.debug('key = %s', key)
    try:
        for run_type in INTERVALS:
            if key in status[run_type]:
                return status[run_type][key][variable]
    except KeyError:
        pass
    LOG.warning('key %s is not found', key)
    return None


def get_galera_version(status, key):
    """
    Get Galera version from backup status

    :param status: backup status
    :type status: dict
    :param key: backup name
    :type key: str
    :return: Galera version or None if not listed in the status
    :rtype: str
    """
    return _get_status_key(status, key, 'wsrep_provider_version')


def get_backup_type(status, key):
    """
    Get backup type - full or incremental - from the backup status.

    :param status: backup status
    :type status: dict
    :param key: backup name
    :type key: str
    :return: Backup type or None if backup name is not found
    :rtype: str
    """
    backup_type = _get_status_key(status, key, 'type')
    if backup_type:
        return backup_type

    raise DestinationError('Unknown backup type for backup copy %s' % key)


def get_my_cnf(status, key):
    """
    Get MySQL config from the status.

    :param status: Backup status.
    :type status: dict
    :param key: Backup name.
    :type key: str
    :return: Content of my.cnf or None if not found
    :rtype: str
    """
    for cnf in _get_status_key(status, key, 'config'):
        k = cnf.keys()[0]
        value = base64.b64decode(cnf[k])
        yield k, value


def restore_from_mysql_full(dst, backup_copy, dst_dir, redo_only=False):
    """
    Restore MySQL datadir from a backup copy

    :param dst: Instance of backup destination.
    :type dst: Destination
    :param backup_copy: Backup copy name.
    :type backup_copy: str
    :param dst_dir: Path to destination directory. Must exist and be empty.
    :type dst_dir: str
    :param redo_only: True if the function has to do final apply of
    the redo log. For example, if you restore backup from a full copy
    it should be False. If you restore from incremental copy and you restore
    base full copy redo_only should be True.
    :type redo_only: bool
    """
    with dst.get_stream(backup_copy) as handler:
        try:
            LOG.debug('Running gunzip')
            gunzip_proc = Popen(['gunzip'],
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
                    LOG.error('%r exited with code %d', xbstream_cmd, ret)
                    if cout:
                        LOG.error('STDOUT: %s', cout)
                    if cerr:
                        LOG.error('STDERR: %s', cerr)
                    exit(1)

            except OSError as err:
                LOG.error('Failed to unarchive %s: %s', backup_copy, err)
                exit(1)
            cout, cerr = gunzip_proc.communicate()
            ret = gunzip_proc.returncode
            if ret:
                LOG.error('gunzip exited with code %d', ret)
                if cout:
                    LOG.error('STDOUT: %s', cout)
                if cerr:
                    LOG.error('STDERR: %s', cerr)
                exit(1)
        except OSError as err:
            LOG.error('Failed to decompress %s: %s', backup_copy, err)
            exit(1)

    mem_usage = psutil.virtual_memory()
    try:
        xtrabackup_cmd = ['innobackupex',
                          '--use-memory=%d' % (mem_usage.available/2),
                          '--apply-log']
        if redo_only:
            xtrabackup_cmd += ['--redo-only']
        xtrabackup_cmd += [dst_dir]

        LOG.debug('Running %s', ' '.join(xtrabackup_cmd))
        xtrabackup_proc = Popen(xtrabackup_cmd,
                                stdout=None,
                                stderr=None)
        xtrabackup_proc.communicate()
        ret = xtrabackup_proc.returncode
        if ret:
            LOG.error('%s exited with code %d', " ".join(xtrabackup_cmd), ret)
    except OSError as err:
        LOG.error('Failed to prepare backup in %s: %s', dst_dir, err)
        exit(1)


def _extract_xbstream(input_stream, working_dir):
    try:
        xbstream_cmd = ['xbstream', '-x']
        xbstream_proc = Popen(xbstream_cmd,
                              stdin=input_stream,
                              stdout=PIPE,
                              stderr=PIPE,
                              cwd=working_dir)
        cout, cerr = xbstream_proc.communicate()
        ret = xbstream_proc.returncode
        if ret:
            LOG.error('%r exited with code %d', xbstream_cmd, ret)
            if cout:
                LOG.error('STDOUT: %s', cout)
            if cerr:
                LOG.error('STDERR: %s', cerr)
            exit(1)

    except OSError as err:
        LOG.error('Failed to extract xbstream: %s', err)
        exit(1)


def restore_from_mysql_incremental(dst, backup_copy, dst_dir):
    """
    Restore MySQL datadir from an incremental copy.

    :param dst: Instance of backup destination.
    :type dst: Destination
    :param backup_copy: Backup copy name
    :type backup_copy: str
    :param dst_dir: Path to destination directory. Must exist and be empty.
    :type dst_dir: str
    """
    full_copy = dst.get_full_copy_name(backup_copy)
    restore_from_mysql_full(dst, full_copy, dst_dir, redo_only=True)
    inc_dir = tempfile.mkdtemp()
    try:
        with dst.get_stream(backup_copy) as handler:
            try:
                LOG.debug('Running gunzip')
                gunzip_proc = Popen(['gunzip'],
                                    stdin=handler,
                                    stdout=PIPE,
                                    stderr=PIPE,
                                    cwd=inc_dir)

                _extract_xbstream(gunzip_proc.stdout, inc_dir)

                cout, cerr = gunzip_proc.communicate()
                ret = gunzip_proc.returncode
                if ret:
                    LOG.error('gunzip exited with code %d', ret)
                    if cout:
                        LOG.error('STDOUT: %s', cout)
                    if cerr:
                        LOG.error('STDERR: %s', cerr)
                    exit(1)
            except OSError as err:
                LOG.error('Failed to decompress %s: %s', backup_copy, err)
                exit(1)

        mem_usage = psutil.virtual_memory()
        try:
            xtrabackup_cmd = ['innobackupex',
                              '--use-memory=%d' % (mem_usage.available / 2),
                              '--apply-log', '--redo-only', dst_dir,
                              '--incremental-dir', inc_dir]
            LOG.debug('Running %s', ' '.join(xtrabackup_cmd))
            xtrabackup_proc = Popen(xtrabackup_cmd,
                                    stdout=None,
                                    stderr=None)
            xtrabackup_proc.communicate()
            ret = xtrabackup_proc.returncode
            if ret:
                LOG.error('%s exited with code %d',
                          " ".join(xtrabackup_cmd),
                          ret)

            xtrabackup_cmd = ['innobackupex',
                              '--use-memory=%d' % (mem_usage.available / 2),
                              '--apply-log', dst_dir]
            LOG.debug('Running %s', ' '.join(xtrabackup_cmd))
            xtrabackup_proc = Popen(xtrabackup_cmd,
                                    stdout=None,
                                    stderr=None)
            xtrabackup_proc.communicate()
            ret = xtrabackup_proc.returncode
            if ret:
                LOG.error('%s exited with code %d',
                          " ".join(xtrabackup_cmd),
                          ret)
        except OSError as err:
            LOG.error('Failed to prepare backup in %s: %s', dst_dir, err)
            exit(1)
    finally:
        try:
            pass
        except OSError as exc:
            if exc.errno != errno.ENOENT:  # ENOENT - no such file or directory
                raise  # re-raise exception


def gen_grastate(path, version, uuid, seqno):
    """
    Generate and save grastate file.

    :param path: Path to grastate file.
    :param version: Galera version from grastate.dat.
    :param uuid: UUID from grastate.dat.
    :param seqno: seqno from grastate.dat.
    """
    with open(path, 'w') as file_desc:
        file_desc.write("""# GALERA saved state
version: {version}
uuid:    {uuid}
seqno:   {seqno}
cert_index:
""".format(version=version, uuid=uuid, seqno=seqno))


def update_grastate(dst_dir, status, key):
    """
    If xtrabackup_galera_info exists in the destination directory
    then parse it and generate grastate.dat file.

    :param dst_dir: Path to destination directory.
    :type dst_dir: str
    :param status: Backup status
    :type status: dict
    :param key: Backup name
    :type key: str
    """
    if os.path.exists(dst_dir + '/xtrabackup_galera_info'):
        version = get_galera_version(status, key)

        with open(dst_dir + '/xtrabackup_galera_info') as galera_info:
            galera_info = galera_info.read()
            uuid = galera_info.split(':')[0]
            seqno = galera_info.split(':')[1]

        gen_grastate(dst_dir + '/grastate.dat',
                     version, uuid, seqno)


def restore_from_mysql(config, backup_copy, dst_dir):
    """
    Restore MySQL datadir in a given directory

    :param config: Tool configuration.
    :type config: ConfigParser.ConfigParser
    :param backup_copy: Backup copy name.
    :type backup_copy: str
    :param dst_dir: Destination directory. Must exist and be empty.
    """
    LOG.info('Restoring %s in %s', backup_copy, dst_dir)
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

    config_dir = os.path.join(dst_dir, "_config")

    for path, content in get_my_cnf(status, key):
        config_sub_dir = os.path.join(config_dir,
                                      os.path.dirname(path).lstrip('/'))
        os.makedirs(config_sub_dir)

        with open(os.path.join(config_sub_dir,
                               os.path.basename(path)), 'w') as mysql_config:
            mysql_config.write(content)

    update_grastate(dst_dir, status, key)

    LOG.info('Successfully restored %s in %s.', backup_copy, dst_dir)
    LOG.info('Now copy content of %s to MySQL datadir: '
             'cp -R %s/* /var/lib/mysql/', dst_dir, dst_dir)
    LOG.info('Fix permissions: chown -R mysql:mysql /var/lib/mysql/')
    LOG.info('Make sure innodb_log_file_size and innodb_log_files_in_group '
             'in %s/backup-my.cnf and in /etc/my.cnf are same.', dst_dir)

    if os.path.exists(config_dir):
        LOG.info('Original my.cnf is restored in %s.', config_dir)

    LOG.info('Then you can start MySQL normally.')


def restore_from_file(config, backup_copy, dst_dir):
    """
    Restore a directory from a backup copy in the directory

    :param config: Tool configuration.
    :type config: ConfigParser.ConfigParser
    :param backup_copy: Backup name.
    :type backup_copy: str
    :param dst_dir: Path to destination directory. Must exist and be empty.
    :type dst_dir: str
    """
    LOG.info('Restoring %s in %s', backup_copy, dst_dir)
    mkdir_p(dst_dir)

    if os.path.exists(backup_copy):
        dst = Local(backup_copy)
    else:
        dst = get_destination(config)

    cmd = "tar zvxf -"
    with dst.get_stream(backup_copy) as handler:
        try:
            LOG.debug('Running %s', cmd)
            proc = Popen(shlex.split(cmd), stdin=handler, cwd=dst_dir)
            cout, cerr = proc.communicate()
            ret = proc.returncode
            if ret:
                LOG.error('%s exited with code %d', cmd, ret)
                if cout:
                    LOG.error('STDOUT: %s', cout)
                if cerr:
                    LOG.error('STDERR: %s', cerr)
                exit(1)
        except OSError as err:
            LOG.error('Failed to decompress %s: %s', backup_copy, err)
            exit(1)
    LOG.info('Successfully restored %s in %s', backup_copy, dst_dir)
