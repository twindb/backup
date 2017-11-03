import mock
import pytest

from twindb_backup.exporter.datadog_exporter import DataDogExporter
from twindb_backup.exporter.exceptions import DataDogExporterError


@mock.patch('twindb_backup.exporter.datadog_exporter.initialize')
def test__datadog_exporter_constructor(mock_initialize):
    exporter = DataDogExporter('foo', 'bar')
    mock_initialize.assert_called_once_with(api_key='bar', app_key='foo')
    assert exporter.metric_name == "twindb.backup_time"


@mock.patch('twindb_backup.exporter.datadog_exporter.statsd')
def test__datadog_exporter_export_int_agument(mock_statsd):
    exporter = DataDogExporter('foo', 'bar')
    exporter.export(1)
    mock_statsd.gauge.assert_called_once_with(exporter.metric_name, 1)


@mock.patch('twindb_backup.exporter.datadog_exporter.statsd')
def test__datadog_exporter_export_float_agument(mock_statsd):
    exporter = DataDogExporter('foo', 'bar')
    exporter.export(1.1)
    mock_statsd.gauge.assert_called_once_with(exporter.metric_name, 1.1)


@mock.patch('twindb_backup.exporter.datadog_exporter.statsd')
def test__datadog_exporter_export_string_agument(mock_statsd):
    exporter = DataDogExporter('foo', 'bar')
    with pytest.raises(DataDogExporterError):
        exporter.export('str')
    mock_statsd.assert_not_called()
