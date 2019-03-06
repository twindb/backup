# -*- coding: utf-8 -*-
"""
Module that works with list of backup copies
"""
from __future__ import print_function
from twindb_backup import LOG, INTERVALS, MEDIA_TYPES
from twindb_backup.destination.local import Local


def list_available_backups(twindb_config, copy_type=None):
    """
    Print known backup copies on a destination specified in the configuration.

    :param twindb_config: tool configuration
    :type twindb_config: TwinDBBackupConfig
    :param copy_type: Limit list to specific type of backups.
    :type copy_type: files|mysql
    """
    dsts = [
        twindb_config.destination()
    ]
    if twindb_config.keep_local_path:
        dsts.insert(
            0,
            Local(twindb_config.keep_local_path)
        )

    for dst in dsts:
        LOG.info('Destination %s', dst)
        for mtype in MEDIA_TYPES:
            if copy_type in [None, mtype]:
                func = "_print_%s" % mtype
                globals()[func](dst)


def _print_files(dst):
    _print_media_type(dst, 'files')


def _print_mysql(dst):
    _print_media_type(dst, 'mysql')


def _print_media_type(dst, media_type):
    for run_type in INTERVALS:
        pattern = "/%s/%s/" % (run_type, media_type)
        dst_files = dst.list_files(
            dst.remote_path,
            pattern=pattern,
            recursive=True,
            files_only=True
        )
        if dst_files:
            LOG.info('%s %s copies:', media_type, run_type)
            for copy in dst_files:
                print(copy)


def _print_binlog(dst):
    dst_files = dst.list_files(
        dst.remote_path,
        pattern='/binlog/',
        recursive=True,
        files_only=True
    )
    if dst_files:
        LOG.info('Binary logs:')
        for copy in dst_files:
            print(copy)
