# -*- coding: utf-8 -*-
from ConfigParser import ConfigParser
import os
import click
from twindb_backup import setup_logging, log
from twindb_backup.backup import run_backup_job
from twindb_backup.ls import list_available_backups
from twindb_backup.restore import restore_from_mysql, restore_from_file

pass_cfg = click.make_pass_decorator(ConfigParser, ensure=True)


@click.group()
@click.option('--debug', help='Print debug messages', is_flag=True,
              default=False)
@click.option('--config', help='TwinDB Backup config file.',
              default='/etc/twindb/twindb-backup.cfg',
              show_default=True)
@pass_cfg
def main(cfg, debug, config):
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
        restore_from_file(cfg, backup_copy, dst)
    else:
        log.info('No backup copy specified. Choose one from below:')
        list_available_backups(cfg)
