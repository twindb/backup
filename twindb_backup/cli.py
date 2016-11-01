# -*- coding: utf-8 -*-
from ConfigParser import ConfigParser
import os
import click
import fcntl
import errno
from twindb_backup import setup_logging, log
from twindb_backup.backup import backup_everything

pass_cfg = click.make_pass_decorator(ConfigParser, ensure=True)
LOCK_FILE = '/var/run/twindb-backup.lock'


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
    try:
        fd = open(LOCK_FILE, 'w+')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        log.debug(run_type)
        if cfg.getboolean('intervals', "run_%s" % run_type):
            backup_everything(run_type, cfg)
        else:
            log.debug('Not running because run_%s is no', run_type)
    except IOError as err:
        if err.errno == errno.EAGAIN:
            log.warning('Another instance of twindb-backup is running?')
        else:
            raise


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
