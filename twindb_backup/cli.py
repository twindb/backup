# -*- coding: utf-8 -*-
"""
Entry points for twindb-backup tool
"""
from __future__ import print_function

import shutil
import socket
import tempfile
import traceback
import os
from os.path import basename

import click

from twindb_backup import setup_logging, LOG, __version__, \
    TwinDBBackupError, LOCK_FILE, INTERVALS, MEDIA_TYPES
from twindb_backup.backup import run_backup_job
from twindb_backup.cache.cache import Cache, CacheException
from twindb_backup.clone import clone_mysql
from twindb_backup.configuration import TwinDBBackupConfig
from twindb_backup.copy.file_copy import FileCopy
from twindb_backup.exceptions import LockWaitTimeoutError, OperationError
from twindb_backup.ls import list_available_backups
from twindb_backup.modifiers.base import ModifierException
from twindb_backup.restore import restore_from_mysql, restore_from_file
from twindb_backup.share import share
from twindb_backup.source.exceptions import SourceError
from twindb_backup.status.binlog_status import BinlogStatus
from twindb_backup.status.mysql_status import MySQLStatus
from twindb_backup.util import ensure_empty, kill_children, \
    get_hostname_from_backup_copy, get_run_type_from_backup_copy
from twindb_backup.verify import verify_mysql_backup

MEDIA_STATUS_MAP = {
    'files': NotImplementedError,
    'mysql': MySQLStatus,
    'binlog': BinlogStatus
}


@click.group(invoke_without_command=True)
@click.option('--debug', help='Print debug messages', is_flag=True,
              default=False)
@click.option('--config', help='TwinDB Backup config file.',
              default='/etc/twindb/twindb-backup.cfg',
              show_default=True)
@click.option('--version', help='Show tool version and exit.', is_flag=True,
              default=False)
@click.option('--xtrabackup-binary',
              help='Path to xtrabackup binary.',
              default=None,
              show_default=True)
@click.option('--xbstream-binary',
              help='Path to xbstream binary.',
              default=None,
              show_default=True)
@click.pass_context
def main(ctx, debug,  # pylint: disable=too-many-arguments
         config, version,
         xtrabackup_binary, xbstream_binary):
    """
    Main entry point

    :param ctx: context (See Click docs (http://click.pocoo.org/6/)
    for explanation)
    :param debug: if True enabled debug logging
    :type debug: bool
    :param config: path to configuration file
    :type config: str
    :param version: If True print version string
    :type version: bool
    :param xtrabackup_binary: Path to xtrabackup binary.
    :type xtrabackup_binary: str
    :param xbstream_binary: Path to xbstream binary.
    :type xbstream_binary: str
    """
    if not ctx.invoked_subcommand:
        if version:
            print(__version__)
            exit(0)
        else:
            print(ctx.get_help())
            exit(-1)

    setup_logging(LOG, debug=debug)

    if os.path.exists(config):
        ctx.obj = {
            'twindb_config': TwinDBBackupConfig(config_file=config)
        }
        if xtrabackup_binary is not None:
            ctx.obj['twindb_config'].mysql.xtrabackup_binary = \
                xtrabackup_binary
        if xbstream_binary is not None:
            ctx.obj['twindb_config'].mysql.xbstream_binary = \
                xbstream_binary
    else:
        LOG.warning("Config file %s doesn't exist", config)


@main.command()
@click.argument(
    'run_type',
    type=click.Choice(INTERVALS)
)
@click.option(
    '--lock-file',
    default=LOCK_FILE,
    show_default=True,
    help='Lock file to protect against multiple backup tool'
         ' instances at same time.'
)
@click.option(
    '--binlogs-only',
    default=False,
    show_default=True,
    is_flag=True,
    help='If specified the tool will copy only MySQL binary logs.'
)
@click.pass_context
def backup(ctx, run_type, lock_file, binlogs_only):
    """Run backup job"""
    try:

        run_backup_job(
            ctx.obj['twindb_config'],
            run_type,
            lock_file=lock_file,
            binlogs_only=binlogs_only
        )
    except (LockWaitTimeoutError, OperationError) as err:
        LOG.error(err)
        LOG.debug(traceback.format_exc())
        exit(1)
    except ModifierException as err:
        LOG.error('Error in modifier class')
        LOG.error(err)
        LOG.debug(traceback.format_exc())
        exit(1)
    except SourceError as err:
        LOG.error(err)
        LOG.debug(traceback.format_exc())
        exit(1)
    except KeyboardInterrupt:
        LOG.info('Exiting...')
        kill_children()
        exit(1)


@main.command(name='ls')
@click.option(
    '--type', 'copy_type',
    type=click.Choice(MEDIA_TYPES),
    default=None
)
@click.pass_context
def list_backups(ctx, copy_type):
    """List available backup copies"""
    list_available_backups(
        ctx.obj['twindb_config'],
        copy_type=copy_type
    )


@main.command(name='share')
@click.argument('s3_url', type=str, required=False)
@click.pass_context
def share_backup(ctx, s3_url):
    """Share backup copy for download"""
    if not s3_url:
        LOG.info('No backup copy specified. Choose one from below:')
        list_available_backups(ctx.obj['twindb_config'])
        exit(1)
    try:
        share(ctx.obj['twindb_config'], s3_url)
    except TwinDBBackupError as err:
        LOG.error(err)
        exit(1)


@main.command()
@click.option(
    '--type', 'copy_type',
    type=click.Choice(MEDIA_TYPES),
    default='mysql'
)
@click.option('--hostname', help='Hostname', show_default=True,
              default=socket.gethostname())
@click.pass_context
def status(ctx, copy_type, hostname):
    """Print backups status"""
    dst = ctx.obj['twindb_config'].destination(backup_source=hostname)
    print(
        dst.status(
            cls=MEDIA_STATUS_MAP[copy_type]
        )
    )


@main.group('restore')
@click.pass_context
def restore(ctx):
    """Restore from backup"""
    LOG.debug('restore: %r', ctx.obj['twindb_config'])


@restore.command('mysql')
@click.argument('backup_copy', required=False)
@click.option('--dst', help='Directory where to restore the backup copy',
              default='.', show_default=True)
@click.option('--cache', help='Save full backup copy in this directory',
              default=None)
@click.pass_context
def restore_mysql(ctx, dst, backup_copy, cache):
    """Restore from mysql backup"""
    LOG.debug('mysql: %r', ctx.obj['twindb_config'])

    if not backup_copy:
        LOG.info('No backup copy specified. Choose one from below:')
        list_available_backups(ctx.obj['twindb_config'])
        exit(1)

    try:
        ensure_empty(dst)
        dst_storage = ctx.obj['twindb_config'].destination(
            backup_source=get_hostname_from_backup_copy(backup_copy)
        )
        key = dst_storage.basename(backup_copy)
        copy = dst_storage.status()[key]
        if cache:
            restore_from_mysql(
                ctx.obj['twindb_config'],
                copy,
                dst,
                cache=Cache(cache)
            )
        else:
            restore_from_mysql(ctx.obj['twindb_config'], copy, dst)

    except (TwinDBBackupError, CacheException) as err:
        LOG.error(err)
        exit(1)
    except (OSError, IOError) as err:
        LOG.error(err)
        exit(1)


@restore.command('file')
@click.argument('backup_copy', required=False)
@click.option('--dst', help='Directory where to restore the backup copy',
              default='.', show_default=True)
@click.pass_context
def restore_file(ctx, dst, backup_copy):
    """Restore from file backup"""
    LOG.debug('file: %r', ctx.obj['twindb_config'])

    if not backup_copy:
        LOG.info('No backup copy specified. Choose one from below:')
        list_available_backups(ctx.obj['twindb_config'])
        exit(1)

    try:
        ensure_empty(dst)
        copy = FileCopy(
            get_hostname_from_backup_copy(backup_copy),
            basename(backup_copy),
            get_run_type_from_backup_copy(backup_copy)
        )
        restore_from_file(ctx.obj['twindb_config'], copy, dst)
    except TwinDBBackupError as err:
        LOG.error(err)
        exit(1)
    except KeyboardInterrupt:
        LOG.info('Exiting...')
        kill_children()
        exit(1)


@main.group('verify')
@click.pass_context
def verify(ctx):
    """Verify backup"""
    LOG.debug('Restore: %r', ctx.obj['twindb_config'])


@verify.command('mysql')
@click.argument('backup_copy', required=False)
@click.option(
    '--dst',
    help='Directory where to restore the backup copy',
    default=tempfile.mkdtemp(),
    show_default=True
)
@click.option(
    '--hostname',
    help='If backup_copy is latest this option '
         'specifies hostname where the backup copy was taken.',
    default=socket.gethostname(),
    show_default=True
)
@click.pass_context
def verify_mysql(ctx, hostname, dst, backup_copy):
    """Verify backup"""
    LOG.debug('mysql: %r', ctx.obj['twindb_config'])

    try:
        if not backup_copy:
            list_available_backups(ctx.obj['twindb_config'])
            exit(1)

        print(
            verify_mysql_backup(
                ctx.obj['twindb_config'],
                dst,
                backup_copy,
                hostname
            )
        )

    finally:

        shutil.rmtree(dst, ignore_errors=True)


@main.group('clone')
@click.pass_context
def clone(ctx):
    """Clone backup on remote server"""
    LOG.debug('Clone: %r', ctx.obj['twindb_config'])


@clone.command('mysql')
@click.argument('source', default='localhost:3306')
@click.argument('destination')
@click.option('--netcat-port', default=9990,
              help='Use this TCP port for netcat file transfers between '
                   'clone source and destination.',
              show_default=True)
@click.option('--replication-user', default='repl',
              help='Replication MySQL user.',
              show_default=True)
@click.option('--replication-password', default='slavepass',
              help='Replication MySQL password.',
              show_default=True)
@click.option('--compress', is_flag=True,
              help='Compress stream while sending it over network.',
              default=False)
@click.pass_context
def clone_mysql_backup(ctx, netcat_port,  # pylint: disable=too-many-arguments
                       replication_user,
                       replication_password,
                       compress,
                       destination, source):
    """
     Clone mysql backup on remote server and make it a slave.
     By default it will take a slave from a local MySQL on port 3306.

     Source and destinations are strings hostname:port.
     E.g. 10.10.10.10:3306.

     If port isn't specified 3306 will be assumed.
    """
    clone_mysql(
        ctx.obj['twindb_config'],
        source,
        destination,
        replication_user,
        replication_password,
        netcat_port=netcat_port,
        compress=compress
    )
