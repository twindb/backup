# -*- coding: utf-8 -*-

"""
Module defines DataDog exporter class.
"""
from datadog import initialize, statsd

from twindb_backup.exporter.base_exporter import BaseExporter
from twindb_backup.exporter.exceptions import DataDogExporterError


class DataDogExporter(BaseExporter):  # pylint: disable=too-few-public-methods
    """
    DataDog exporter class
    """
    def __init__(self, app_key, api_key,
                 metric_name="twindb.backup_time"):
        super(DataDogExporter, self).__init__()
        options = {
            'api_key': api_key,
            'app_key': app_key
        }
        initialize(**options)
        self.metric_name = metric_name

    def export(self, data):
        """
        Export data to DataDog

        :param data: Data to posting
        :raise: DataDogExporterError if data is invalid
        """
        if isinstance(data, (int, float, long)):
            statsd.gauge(self.metric_name, data)
        else:
            raise DataDogExporterError("Invalid input data")
