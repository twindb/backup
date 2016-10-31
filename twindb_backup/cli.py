# -*- coding: utf-8 -*-
from ConfigParser import ConfigParser, NoOptionError
import os
import click
from twindb_backup import setup_logging, log
from twindb_backup.backup import backup_everything

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
    log.debug(run_type)
    #try:
    if cfg.getboolean('intervals', "run_%s" % run_type):
        backup_everything(run_type, cfg)
    #except NoOptionError:
    #    log.error('Unknown run type %s', run_type)


@main.command()
@pass_cfg
def ls(cfg):
    """List available backup copies"""
    log.debug('%r', cfg)


@main.command()
@pass_cfg
def restore(cfg):
    """Restore from backup"""
    log.debug('%r', cfg)

