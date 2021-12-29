import mock
import pytest

from twindb_backup.exporter.base_exporter import ExportCategory, ExportMeasureType
from twindb_backup.exporter.stasd_exporter import StatsdExporter
from twindb_backup.exporter.exceptions import StatsdExporterError


@mock.patch('twindb_backup.exporter.stasd_exporter.initialize')
def test__stasd_exporter_constructor(mock_initialize):
    exporter = StatsdExporter('localhost', 8125)
    mock_initialize.assert_called_once_with(statsd_host='localhost', statsd_port=8125)
    assert exporter._suffix == "twindb."
