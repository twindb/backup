"""
Module defines DataDog exporter class.
"""
from twindb_backup.exporter.base_exporter import BaseExporter
from datadog import initialize, statsd


class DataDogExporter(BaseExporter):

    def __init__(self, app_key, api_key,
                 metric_name="twindb.backup_time"):
        super(BaseExporter, self).__init__()
        options = {
            'api_key': api_key,
            'app_key': app_key
        }
        initialize(**options)
        self.metric_name = metric_name

    def export(self, data):
        statsd.gauge(self.metric_name, data)
