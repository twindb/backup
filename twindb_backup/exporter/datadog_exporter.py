"""
Module defines DataDog exporter class.
"""
from twindb_backup.exporter.base_exporter import BaseExporter
from datadog import initialize


class DataDogExporter(BaseExporter):

    def __init__(self, app_key, api_key):
        super(BaseExporter, self).__init__()
        options = {
            'api_key': api_key,
            'app_key': app_key
        }
        initialize(**options)

    def export(self, data):
        pass
