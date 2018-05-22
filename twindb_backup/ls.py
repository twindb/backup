# -*- coding: utf-8 -*-
"""
Module that works with list of backup copies
"""
from __future__ import print_function
from ConfigParser import NoOptionError, NoSectionError
import os
from subprocess import Popen
from twindb_backup import LOG
from twindb_backup.backup import get_destination


def list_available_backups(config, run_type=None, copy_type=None):
    """
    Print known backup copies on a destination specified in the configuration.

    :param config: tool configuration
    :type config: ConfigParser.ConfigParser
    :param run_type: Interval of backup
    :param copy_type: Backup copy type
    """
    try:
        keep_local_path = config.get('destination', 'keep_local_path')
        LOG.info('Local copies:')

        if os.path.exists(keep_local_path):
            cmd = ["find", keep_local_path, '-type', 'f']
            LOG.debug('Running %s', ' '.join(cmd))
            proc = Popen(cmd)
            proc.communicate()
    except (NoOptionError, NoSectionError):
        pass
    dst = get_destination(config)

    if run_type is None and copy_type is None:
        for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
            LOG.info('%s copies:', run_type)
            for copy in dst.get_files(prefix=dst.remote_path,
                                      interval=run_type):
                print(copy)
    else:
        for copy in dst.get_files(prefix=dst.remote_path,
                                  interval=run_type,
                                  copy_type=copy_type):
            print(copy)
