from twindb_backup.configuration import TwinDBBackupConfig
from twindb_backup.configuration.run_intervals import RunIntervals


def test_run_intervals(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.run_intervals == RunIntervals()
