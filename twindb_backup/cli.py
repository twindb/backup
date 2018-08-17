# -*- coding: utf-8 -*-
"""
Entry points for twindb-backup tool
"""
from __future__ import print_function

import traceback
from ConfigParser import ConfigParser, NoSectionError
import os
import click

from twindb_backup import setup_logging, LOG, __version__, \
    TwinDBBackupError, LOCK_FILE, XTRABACKUP_BINARY, XBSTREAM_BINARY, INTERVALS
from twindb_backup.backup import run_backup_job
from twindb_backup.cache.cache import Cache, CacheException
from twindb_backup.clone import clone_mysql
from twindb_backup.configuration import get_destination
from twindb_backup.exceptions import LockWaitTimeoutError, OperationError
from twindb_backup.ls import list_available_backups
from twindb_backup.modifiers.base import ModifierException
from twindb_backup.restore import restore_from_mysql, restore_from_file
from twindb_backup.share import share
from twindb_backup.util import ensure_empty, kill_children
from twindb_backup.verify import verify_mysql_backup

PASS_CFG = click.make_pass_decorator(ConfigParser, ensure=True)


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
              default=XTRABACKUP_BINARY,
              show_default=True)
@click.option('--xbstream-binary',
              help='Path to xbstream binary.',
              default=XBSTREAM_BINARY,
              show_default=True)
@PASS_CFG
@click.pass_context
def main(ctx, cfg, debug,  # pylint: disable=too-many-arguments
         config, version,
         xtrabackup_binary=XTRABACKUP_BINARY,
         xbstream_binary=XBSTREAM_BINARY):
    """
    Main entry point

    :param ctx: context (See Click docs (http://click.pocoo.org/6/)
    for explanation)
    :param cfg: instance of ConfigParser
    :type cfg: ConfigParser.ConfigParser
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
        cfg.read(config)
        try:
            cfg.set('mysql', 'xtrabackup_binary', xtrabackup_binary)
            cfg.set('mysql', 'xbstream_binary', xbstream_binary)
        except NoSectionError:
            # if there is no mysql section, we will not backup mysql
            pass
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
@PASS_CFG
def backup(cfg, run_type, lock_file):
    """Run backup job"""
    try:

        run_backup_job(cfg, run_type, lock_file=lock_file)
    except (LockWaitTimeoutError, OperationError) as err:
        LOG.error(err)
        LOG.debug(traceback.format_exc())
        exit(1)
    except ModifierException as err:
        LOG.error('Error in modifier class')
        LOG.error(err)
        LOG.debug(traceback.format_exc())
        exit(1)
    except KeyboardInterrupt:
        LOG.info('Exiting...')
        kill_children()
        exit(1)


@main.command(name='ls')
@PASS_CFG
def list_backups(cfg):
    """List available backup copies"""
    list_available_backups(cfg)


@main.command(name='share')
@click.argument('s3_url', type=str, required=False)
@PASS_CFG
def share_backup(cfg, s3_url):
    """Share backup copy for download"""
    if not s3_url:
        LOG.info('No backup copy specified. Choose one from below:')
        list_available_backups(cfg)
        exit(1)
    try:
        share(cfg, s3_url)
    except TwinDBBackupError as err:
        LOG.error(err)
        exit(1)


@main.command()
@PASS_CFG
def status(cfg):
    """Print backups status"""
    dst = get_destination(cfg)
    print(dst.status())


@main.group('restore')
@PASS_CFG
def restore(cfg):
    """Restore from backup"""
    LOG.debug('restore: %r', cfg)


@restore.command('mysql')
@click.argument('backup_copy', required=False)
@click.option('--dst', help='Directory where to restore the backup copy',
              default='.', show_default=True)
@click.option('--cache', help='Save full backup copy in this directory',
              default=None)
@PASS_CFG
def restore_mysql(cfg, dst, backup_copy, cache):
    """Restore from mysql backup"""
    LOG.debug('mysql: %r', cfg)

    if not backup_copy:
        LOG.info('No backup copy specified. Choose one from below:')
        list_available_backups(cfg)
        exit(1)

    try:
        ensure_empty(dst)
        if cache:
            restore_from_mysql(cfg, backup_copy, dst, cache=Cache(cache))
        else:
            restore_from_mysql(cfg, backup_copy, dst)

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
@PASS_CFG
def restore_file(cfg, dst, backup_copy):
    """Restore from file backup"""
    LOG.debug('file: %r', cfg)

    if not backup_copy:
        LOG.info('No backup copy specified. Choose one from below:')
        list_available_backups(cfg)
        exit(1)

    try:
        ensure_empty(dst)
        restore_from_file(cfg, backup_copy, dst)
    except TwinDBBackupError as err:
        LOG.error(err)
        exit(1)
    except KeyboardInterrupt:
        LOG.info('Exiting...')
        kill_children()
        exit(1)


@main.group('verify')
@PASS_CFG
def verify(cfg):
    """Verify backup"""
    LOG.debug('Restore: %r', cfg)


@verify.command('mysql')
@click.argument('backup_copy', required=False)
@click.option('--dst', help='Directory where to restore the backup copy',
              default='/tmp/', show_default=True)
@click.option('--hostname', help='If backup_copy is '
                                 'latest this option specifies hostname '
                                 'where the backup copy was taken.',
              show_default=True)
@PASS_CFG
def verify_mysql(cfg, hostname, dst, backup_copy):
    """Verify backup"""
    LOG.debug('mysql: %r', cfg)

    if not backup_copy:
        list_available_backups(cfg)
        exit(1)

    print(verify_mysql_backup(cfg, dst, backup_copy, hostname))


@main.group('clone')
@PASS_CFG
def clone(cfg):
    """Clone backup on remote server"""
    LOG.debug('Clone: %r', cfg)


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
@PASS_CFG
def clone_mysql_backup(cfg, netcat_port,  # pylint: disable=too-many-arguments
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
    clone_mysql(cfg, source, destination,
                replication_user, replication_password,
                netcat_port=netcat_port,
                compress=compress)
