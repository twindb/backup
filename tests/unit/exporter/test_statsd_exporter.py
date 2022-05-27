import mock
from statsd import StatsClient

from twindb_backup.exporter.statsd_exporter import StatsdExporter


def test_stasd_exporter_constructor():
    with mock.patch.object(
        StatsClient, "__init__", return_value=None
    ) as mock_StatsClient_init:
        exporter = StatsdExporter("localhost", 8125)
        assert exporter._suffix == "twindb."
        assert isinstance(exporter._client, StatsClient)
        mock_StatsClient_init.assert_called_once_with("localhost", 8125)
