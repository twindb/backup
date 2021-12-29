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


@pytest.mark.parametrize('category, measure_type, metric_name', [
    (ExportCategory.mysql, ExportMeasureType.restore, "twindb.mysql.restore_time"),
    (ExportCategory.files, ExportMeasureType.restore, "twindb.files.restore_time"),
    (ExportCategory.mysql, ExportMeasureType.backup, "twindb.mysql.backup_time"),
    (ExportCategory.files, ExportMeasureType.backup, "twindb.files.backup_time"),
])
@mock.patch('twindb_backup.exporter.stasd_exporter.statsd')
def test__stasd_exporter_export_int_agument(mock_statsd,
                                              category,
                                              measure_type,
                                              metric_name):
    exporter = StatsdExporter('localhost', 8125)
    exporter.export(category, measure_type, 1)
    mock_statsd.gauge.assert_called_once_with(metric_name, 1)


@pytest.mark.parametrize('category, measure_type, metric_name', [
    (ExportCategory.mysql, ExportMeasureType.restore, "twindb.mysql.restore_time"),
    (ExportCategory.files, ExportMeasureType.restore, "twindb.files.restore_time"),
    (ExportCategory.mysql, ExportMeasureType.backup, "twindb.mysql.backup_time"),
    (ExportCategory.files, ExportMeasureType.backup, "twindb.files.backup_time"),
])
@mock.patch('twindb_backup.exporter.stasd_exporter.statsd')
def test__stasd_exporter_export_float_agument(mock_statsd,
                                                category,
                                                measure_type,
                                                metric_name):
    exporter = StatsdExporter('localhost', 8125)
    exporter.export(category, measure_type, 1.1)
    mock_statsd.gauge.assert_called_once_with(metric_name, 1.1)


@pytest.mark.parametrize('category, measure_type, metric_name', [
    (ExportCategory.mysql, ExportMeasureType.restore, "twindb.mysql.restore_time"),
    (ExportCategory.files, ExportMeasureType.restore, "twindb.files.restore_time"),
    (ExportCategory.mysql, ExportMeasureType.backup, "twindb.mysql.backup_time"),
    (ExportCategory.files, ExportMeasureType.backup, "twindb.files.backup_time"),
])
@mock.patch('twindb_backup.exporter.stasd_exporter.statsd')
def test__stasd_exporter_export_string_agument(mock_statsd,
                                                 category,
                                                 measure_type,
                                                 metric_name):
    exporter = StatsdExporter('localhost', 8125)
    with pytest.raises(StatsdExporterError):
        exporter.export(category, measure_type, "str")
    mock_statsd.assert_not_called()
