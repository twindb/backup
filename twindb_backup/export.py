# -*- coding: utf-8 -*-
"""
Module to process export
"""
from twindb_backup.configuration import TwinDBBackupConfig


def export_info(cfg, data, category, measure_type):
    """
    Export data to service

    :param cfg: Config file
    :param data: Data
    :param category: Category of data
    :param measure_type: Type of measure
    :param category: Category
    """

    transport = TwinDBBackupConfig(config_file=cfg).exporter

    if transport:
        transport.export(
            category=category,
            measure_type=measure_type,
            data=data
        )
