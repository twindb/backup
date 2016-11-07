# -*- coding: utf-8 -*-
from ConfigParser import ConfigParser
import os
import click
from twindb_backup import setup_logging, log
from twindb_backup.backup import run_backup_job
from twindb_backup.ls import list_available_backups

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


@main.command()
@pass_cfg
def restore(cfg):
    """Restore from backup"""
    log.debug('%r', cfg)
