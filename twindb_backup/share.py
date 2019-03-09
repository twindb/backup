# -*- coding: utf-8 -*-
"""
Module that works with sharing backups
"""
from __future__ import print_function

from twindb_backup import TwinDBBackupError


def share(twindb_config, s3_url):
    """
    Function for generate make public file and get public url

    :param twindb_config: tool configuration
    :type twindb_config: TwinDBBackupConfig
    :param s3_url: S3 url to file
    :type s3_url: str
    :raise: TwinDBBackupError
    """
    try:
        print(twindb_config.destination().share(s3_url))
    except NotImplementedError as err:
        raise TwinDBBackupError(err)
