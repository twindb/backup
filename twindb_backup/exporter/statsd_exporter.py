# -*- coding: utf-8 -*-

"""
Module defines Statsd exporter class.
"""
import statsd


from twindb_backup.exporter.base_exporter import (
    BaseExporter,
    ExportCategory,
    ExportMeasureType,
)
from twindb_backup.exporter.exceptions import StatsdExporterError


class StatsdExporter(BaseExporter):  # pylint: disable=too-few-public-methods
    """
    Statsd exporter class
    """

    def __init__(self, statsd_host, statsd_port):
        super(StatsdExporter, self).__init__()
        self._client = statsd.StatsClient(statsd_host, statsd_port)
        self._suffix = "twindb."

    def export(self, category, measure_type, data):
        """
        Export data to StatsD server
        :param category: Data meant
        :param measure_type: Type of measure
        :param data: Data to posting
        :raise: StatsdExporterError if data is invalid
        """
        if isinstance(data, (int, float)):
            metric_name = self._suffix
            if category == ExportCategory.files:
                metric_name += "files."
            else:
                metric_name += "mysql."
            if measure_type == ExportMeasureType.backup:
                metric_name += "backup_time"
            else:
                metric_name += "restore_time"
            self._client.timing(metric_name, data)
        else:
            raise StatsdExporterError("Invalid input data")
