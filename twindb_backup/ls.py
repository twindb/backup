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


def list_available_backups(config):
    """
    Print known backup copies on a destination specified in the configuration.

    :param config: tool configuration
    :type config: ConfigParser.ConfigParser
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

    for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
        LOG.info('%s copies:', run_type)
        dst = get_destination(config)

        for copy in dst.find_files(dst.remote_path, run_type):
            print(copy)
