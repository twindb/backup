# -*- coding: utf-8 -*-
"""
Module that verify backup copies.
"""
import json
import shutil
import time
import tempfile
import traceback
from configparser import ConfigParser, NoOptionError

from twindb_backup import LOG
from twindb_backup.exceptions import TwinDBBackupError
from twindb_backup.restore import restore_from_mysql
from twindb_backup.status.mysql_status import MySQLStatus


def edit_backup_my_cnf(dst_path):
    """Removed options from config(besides MySQL 5.7.8)"""
    filename = "{dir}/backup-my.cnf".format(dir=dst_path)
    backup_cfg = ConfigParser()
    backup_cfg.read(filename)
    for option in [
        "innodb_log_checksum_algorithm",
        "innodb_log_block_size",
        "innodb_fast_checksum",
    ]:
        try:
            backup_cfg.remove_option(section="mysqld", option=option)
        except NoOptionError:
            pass
    with open(filename, "w") as backup_fp:
        backup_cfg.write(backup_fp)


def verify_mysql_backup(twindb_config, dst_path, backup_file, hostname=None):
    """
    Restore mysql backup and measure time

    :param hostname:
    :param backup_file:
    :param dst_path:
    :param twindb_config: tool configuration
    :type twindb_config: TwinDBBackupConfig

    """
    dst = twindb_config.destination(backup_source=hostname)
    status = MySQLStatus(dst=dst)
    copy = None

    if backup_file == "latest":
        copy = status.latest_backup
    else:
        for copy in status:
            if backup_file.endswith(copy.key):
                break
    if copy is None:
        return json.dumps(
            {"backup_copy": backup_file, "restore_time": 0, "success": False},
            indent=4,
            sort_keys=True,
        )
    start_restore_time = time.time()
    success = True
    tmp_dir = tempfile.mkdtemp()

    try:

        LOG.debug("Verifying backup copy in %s", tmp_dir)
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
    return json.dumps(
        {
            "backup_copy": copy.key,
            "restore_time": restore_time,
            "success": success,
        },
        indent=4,
        sort_keys=True,
    )
