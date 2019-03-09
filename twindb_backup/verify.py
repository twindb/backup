# -*- coding: utf-8 -*-
"""
Module that verify backup copies.
"""
import json
import shutil
import time
import ConfigParser
import tempfile
import traceback

from twindb_backup import LOG, TwinDBBackupError
from twindb_backup.restore import restore_from_mysql


def edit_backup_my_cnf(dst_path):
    """Removed options from config(besides MySQL 5.7.8)"""
    filename = "{dir}/backup-my.cnf".format(dir=dst_path)
    backup_cfg = ConfigParser.ConfigParser()
    backup_cfg.read(filename)
    for option in ['innodb_log_checksum_algorithm', 'innodb_log_block_size',
                   'innodb_fast_checksum']:
        try:
            backup_cfg.remove_option(section='mysqld', option=option)
        except ConfigParser.NoOptionError:
            pass
    with open(filename, 'w') as backup_fp:
        backup_cfg.write(backup_fp)


def verify_mysql_backup(twindb_config, dst_path, backup_copy, hostname=None):
    """
    Restore mysql backup and measure time

    :param hostname:
    :param backup_copy:
    :param dst_path:
    :param twindb_config: tool configuration
    :type twindb_config: TwinDBBackupConfig

    """
    dst = twindb_config.destination(backup_source=hostname)
    status = dst.status()
    if backup_copy == "latest":
        copy = status.get_latest_backup()
    else:
        key = dst.basename(backup_copy)
        copy = status[key]

    if copy is None:
        return json.dumps({
            'backup_copy': backup_copy,
            'restore_time': 0,
            'success': False
        }, indent=4, sort_keys=True)
    start_restore_time = time.time()
    success = True
    tmp_dir = tempfile.mkdtemp()

    try:

        LOG.debug('Verifying backup copy in %s', tmp_dir)
        restore_from_mysql(twindb_config, copy, dst_path, tmp_dir)
        edit_backup_my_cnf(dst_path)

    except (TwinDBBackupError, OSError, IOError) as err:

        LOG.error(err)
        LOG.debug(traceback.format_exc())
        success = False

    finally:

        shutil.rmtree(tmp_dir, ignore_errors=True)

    end_restore_time = time.time()
    restore_time = end_restore_time - start_restore_time
    return json.dumps({
        'backup_copy': copy.key,
        'restore_time': restore_time,
        'success': success
    }, indent=4, sort_keys=True)
