"""Class to store and work with status file"""
from __future__ import print_function

import json
from base64 import b64decode, b64encode


from twindb_backup import INTERVALS, LOG
from twindb_backup.copy.mysql_copy import MySQLCopy


# For backward compatibility content of my.cnf files is base64 encoded.
from twindb_backup.status.exceptions import CorruptedStatus
from twindb_backup.status.periodic_status import PeriodicStatus


class MySQLStatus(PeriodicStatus):
    """
    Class that stores status file and implements operations on it.
    """

    def _status_serialize(self):

        def _serialize_config_dict(cfg):
            config_serialized = []
            for key, value in cfg.iteritems():
                config_serialized.append(
                    {
                        key: b64encode(value)
                    }
                )
            return config_serialized

        status = {}
        for interval in INTERVALS:
            status[interval] = {}
            copies = getattr(self, interval)
            for _, copy in copies.iteritems():

                status[interval][copy.key] = copy.as_dict()
                status[interval][copy.key]['config'] = _serialize_config_dict(
                    status[interval][copy.key]['config']
                )

        return b64encode(
            json.dumps(status)
        )

    def _load(self, status_as_json):
        status = []
        try:
            status_as_obj = json.loads(status_as_json)
        except ValueError:
            raise CorruptedStatus(
                'Could not load status from a bad JSON string %s'
                % (status_as_json, )
            )

        for run_type in INTERVALS:
            for key, value in status_as_obj[run_type].iteritems():

                try:
                    host = key.split('/')[0]
                    file_name = key.split('/')[3]
                    kwargs = {
                        'type': value['type'],
                        'config': self.__serialize_config(value)
                    }
                    keys = [
                        'backup_started',
                        'backup_finished',
                        'binlog',
                        'parent',
                        'lsn',
                        'position',
                        'wsrep_provider_version',
                    ]
                    for copy_key in keys:
                        if copy_key in value:
                            kwargs[copy_key] = value[copy_key]

                    copy = MySQLCopy(
                        host,
                        run_type,
                        file_name,
                        **kwargs
                    )
                    status.append(copy)
                except IndexError as err:
                    LOG.error(err)
                    raise CorruptedStatus('Unexpected key %s' % key)

        return status

    def __init__(self, content=None):
        super(MySQLStatus, self).__init__(content=content)

    # def __repr__(self):
    #     return json.dumps(self.as_dict(), indent=4, sort_keys=True)

    def next_backup_type(self, full_backup, run_type):
        """
        Return backup type to take. If full_backup=daily then
        for hourly backups it will be incremental, for all other - full

        :param full_backup: when to take full backup according to config.
        :param run_type: what kind of backup run it is.
        :return: "full" or "incremental"
        :rtype: str
        """
        if INTERVALS.index(full_backup) <= INTERVALS.index(run_type):
            return "full"
        elif not self.full_copy_exists(run_type):
            return "full"
        else:
            return "incremental"

    def candidate_parent(self, run_type):
        """
        Find a backup copy that can be a parent

        :param run_type: See :func:`~get_backup_type`.
        :return: Backup copy or None
        :rtype: MySQLCopy
        """
        full_backup_index = INTERVALS.index(run_type)
        LOG.debug('Looking a parent candidate for %s run', run_type)
        for i in xrange(full_backup_index, len(INTERVALS)):
            period_copies = getattr(self, INTERVALS[i])
            LOG.debug(
                'Checking %d %s copies',
                len(period_copies),
                INTERVALS[i]
            )
            for _, value in period_copies.iteritems():
                try:
                    if value.type == 'full':
                        LOG.debug('Found parent %r', value)
                        return value
                except KeyError:
                    return None
        LOG.debug('No eligible parents')
        return None

    def full_copy_exists(self, run_type):
        """
        Check whether there is a full copy.

        :param run_type: See :func:`~get_backup_type`.
        :return: True if there is a full copy. False if there is no
            an eligible full copy.
        :rtype: bool
        """
        return self.candidate_parent(run_type) is not None

    @staticmethod
    def __serialize_config(copy):
        config = {}
        try:
            for cfg in copy['config']:
                for cfg_key, cfg_value in cfg.iteritems():
                    config[cfg_key] = b64decode(cfg_value)
        except KeyError:
            config = {}

        return config
