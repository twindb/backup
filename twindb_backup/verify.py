# -*- coding: utf-8 -*-
"""
Module that verify backup copies.
"""
import json
import time
import ConfigParser

from twindb_backup import LOG, TwinDBBackupError
from twindb_backup.configuration import get_destination
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


def verify_mysql_backup(config, dst_path, backup_copy, hostname=None):
    """Restore mysql backup and measure time"""
    if backup_copy == "latest":
        dst = get_destination(config, hostname)
        url = dst.get_latest_backup()
    else:
        url = backup_copy
    start_restore_time = time.time()
    success = True
    try:
        restore_from_mysql(config, url, dst_path)
    except (TwinDBBackupError, OSError, IOError) as err:
        LOG.error(err)
        success = False
    end_restore_time = time.time()
    restore_time = end_restore_time - start_restore_time
    data = {
        'backup_copy': url,
        'restore_time': restore_time,
        'success': success
    }
    if success:
        edit_backup_my_cnf(dst_path)
    return json.dumps(data)
