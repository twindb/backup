# -*- coding: utf-8 -*-
"""
Entry points for twindb-backup tool
"""
from __future__ import print_function
from ConfigParser import ConfigParser
import json
import os
import click
from twindb_backup import setup_logging, LOG, __version__, TwinDBBackupError
from twindb_backup.backup import run_backup_job
from twindb_backup.cache.cache import Cache, CacheException
from twindb_backup.clone import clone_mysql
from twindb_backup.configuration import get_destination
from twindb_backup.ls import list_available_backups
from twindb_backup.restore import restore_from_mysql, restore_from_file
from twindb_backup.share import share
from twindb_backup.util import ensure_empty
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
@PASS_CFG
@click.pass_context
def main(ctx, cfg, debug, config, version):
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
    else:
        LOG.warning("Config file %s doesn't exist", config)


@main.command()
@click.argument('run_type',
                type=click.Choice(['hourly', 'daily', 'weekly',
                                   'monthly', 'yearly']))
@PASS_CFG
def backup(cfg, run_type):
    """Run backup job"""

    run_backup_job(cfg, run_type)


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
    print(json.dumps(dst.status(), indent=4, sort_keys=True))


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
            restore_from_mysql(cfg, backup_copy, dst, Cache(cache))
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
@click.argument('source_host', required=False)
@click.argument('destination_host', required=False)
@click.option('--binary_logging', help='Enable binary logging', is_flag=True,
              default=True)
@click.option('--server_id', help='Server id for replication', default="slave")
@PASS_CFG
def clone_mysql_backup(cfg, server_id, binary_logging, destination_host, source_host):
    """"""
    if not source_host:
        LOG.info('No source_host specified')
        exit(1)
    if not destination_host:
        LOG.info('No destination_host specified')
        exit(1)
    clone_mysql(cfg, destination_host, source_host, server_id, binary_logging)
