# -*- coding: utf-8 -*-
"""
Module that works with list of backup copies
"""
from __future__ import print_function
from ConfigParser import NoOptionError, NoSectionError
import os
from subprocess import Popen
from twindb_backup import LOG, INTERVALS
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
    dst = get_destination(config)
    for run_type in INTERVALS:
        LOG.info('%s copies:', run_type)
        dst_files = dst.list_files(
            dst.remote_path,
            pattern="/%s/" % run_type,
            recursive=True,
            files_only=True
        )
        for copy in dst_files:
            print(copy)
