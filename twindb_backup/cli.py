# -*- coding: utf-8 -*-
from ConfigParser import ConfigParser
import json
import os
import click
from twindb_backup import setup_logging, log, __version__
from twindb_backup.backup import run_backup_job
from twindb_backup.ls import list_available_backups
from twindb_backup.restore import restore_from_mysql, restore_from_file
from twindb_backup.util import get_destination

pass_cfg = click.make_pass_decorator(ConfigParser, ensure=True)


@click.group(invoke_without_command=True)
@click.option('--debug', help='Print debug messages', is_flag=True,
              default=False)
@click.option('--config', help='TwinDB Backup config file.',
              default='/etc/twindb/twindb-backup.cfg',
              show_default=True)
@click.option('--version', help='Show tool version and exit.', is_flag=True,
              default=False)
@pass_cfg
@click.pass_context
def main(ctx, cfg, debug, config, version):
    if not ctx.invoked_subcommand:
        if version:
            print(__version__)
            exit(0)
        else:
            print(ctx.get_help())
            exit(-1)

    setup_logging(log, debug=debug)

    if os.path.exists(config):
        cfg.read(config)
    else:
        log.error("Config file %s doesn't exist", config)
        exit(1)

    pass


@main.command()
@click.argument('run_type',
                type=click.Choice(['hourly', 'daily', 'weekly',
                                   'monthly', 'yearly']))
@pass_cfg
def backup(cfg, run_type):
    """Run backup job"""

    run_backup_job(cfg, run_type)


@main.command()
@pass_cfg
def ls(cfg):
    """List available backup copies"""
    list_available_backups(cfg)


@main.command()
@pass_cfg
def status(cfg):
    """Print backups status"""
    dst = get_destination(cfg)
    print(json.dumps(dst.status(), indent=4, sort_keys=True))


@main.group('restore')
@pass_cfg
def restore(cfg):
    """Restore from backup"""
    log.debug('restore: %r', cfg)


@restore.command('mysql')
@click.argument('backup_copy', required=False)
@click.option('--dst', help='Directory where to restore the backup copy',
              default='.', show_default=True)
@pass_cfg
def restore_mysql(cfg, dst, backup_copy):
    """Restore from mysql backup"""
    log.debug('mysql: %r', cfg)

    if backup_copy:
        ensure_empty(dst)
        restore_from_mysql(cfg, backup_copy, dst)
    else:
        log.info('No backup copy specified. Choose one from below:')
        list_available_backups(cfg)


@restore.command('file')
@click.argument('backup_copy', required=False)
@click.option('--dst', help='Directory where to restore the backup copy',
              default='.', show_default=True)
@pass_cfg
def restore_file(cfg, dst, backup_copy):
    """Restore from file backup"""
    log.debug('file: %r', cfg)

    if backup_copy:
        ensure_empty(dst)
        restore_from_file(cfg, backup_copy, dst)
    else:
        log.info('No backup copy specified. Choose one from below:')
        list_available_backups(cfg)


def ensure_empty(path):
    try:
        if os.listdir(path):
            log.error('Directory %s is not empty' % path)
            exit(1)
    except OSError as err:
        if err.errno == 2:  # OSError: [Errno 2] No such file or directory
            pass
        else:
            raise
