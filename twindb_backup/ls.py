# -*- coding: utf-8 -*-
"""
Module that works with list of backup copies
"""
from __future__ import print_function
from twindb_backup import LOG, INTERVALS
from twindb_backup.backup import get_destination
from twindb_backup.destination.local import Local


def list_available_backups(config, copy_type=None):
    """
    Print known backup copies on a destination specified in the configuration.

    :param config: tool configuration
    :type config: ConfigParser.ConfigParser
    :param copy_type: Limit list to specific type of backups.
    :type copy_type: files|mysql
    """
    dsts = [
        get_destination(config)
    ]
    if config.has_option('destination', 'keep_local_path'):
        LOG.info('Local copies:')
        dsts.insert(
            0,
            Local(
                config.get('destination', 'keep_local_path')
            )
        )

    for dst in dsts:
        LOG.info('Files on %s', dst)
        for run_type in INTERVALS:
            pattern = "/%s/" % run_type
            if copy_type:
                pattern += copy_type + '/'

            dst_files = dst.list_files(
                dst.remote_path,
                pattern=pattern,
                recursive=True,
                files_only=True
            )
            if dst_files:
                LOG.info('%s copies:', run_type)
                for copy in dst_files:
                    print(copy)
