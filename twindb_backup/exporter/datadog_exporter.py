# -*- coding: utf-8 -*-

"""
Module defines DataDog exporter class.
"""
from datadog import initialize, statsd

from twindb_backup.exporter.base_exporter import BaseExporter, \
    ExportCategory, ExportMeasureType
from twindb_backup.exporter.exceptions import DataDogExporterError


class DataDogExporter(BaseExporter):  # pylint: disable=too-few-public-methods
    """
    DataDog exporter class
    """
    def __init__(self, app_key, api_key):
        super(DataDogExporter, self).__init__()
        options = {
            'api_key': api_key,
            'app_key': app_key
        }
        initialize(**options)
        self._suffix = "twindb."

    def export(self, category, measure_type, data):
        """
        Export data to DataDog
        :param category: Data meant
        :param measure_type: Type of measure
        :param data: Data to posting
        :raise: DataDogExporterError if data is invalid
        """
        if isinstance(data, (int, float, long)):
            metric_name = self._suffix
            if category == ExportCategory.files:
                metric_name += "files."
            else:
                metric_name += "mysql."
            if measure_type == ExportMeasureType.backup:
                metric_name += "backup_time"
            else:
                metric_name += "restore_time"
            statsd.gauge(metric_name, data)
        else:
            raise DataDogExporterError("Invalid input data")
