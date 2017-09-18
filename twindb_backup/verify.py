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


def get_latest_backup(dst):
    """Get latest backup path"""

    status = dst.status()
    daily_status = status['daily']
    hourly_status = status['hourly']
    hourly_last_filename = None
    daily_last_filename = None
    try:
        hourly_list = hourly_status.keys()
        hourly_list.sort()
        hourly_last_filename = hourly_list[-1].split("/")[-1]
    except IndexError:
        pass
    try:
        daily_list = daily_status.keys()
        daily_list.sort()
        daily_last_filename = daily_list[-1].split("/")[-1]
    except IndexError:
        pass
    if hourly_last_filename > daily_last_filename:
        filename = hourly_status.keys()[-1]
    elif hourly_last_filename < daily_last_filename:
        filename = daily_status.keys()[-1]
    else:
        raise TwinDBBackupError("Latest backup not found")
    url = "{remote_path}/{filename}".format(remote_path=dst.remote_path, filename=filename)
    LOG.info("Last daily backup is %s", url)
    return url


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
        url = get_latest_backup(dst)
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
    data = {}
    data['backup_copy'] = url
    data['restore_time'] = restore_time
    data['success'] = success
    if success:
        edit_backup_my_cnf(dst_path)
    return json.dumps(data)
