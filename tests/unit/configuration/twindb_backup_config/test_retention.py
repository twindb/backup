from twindb_backup.configuration import TwinDBBackupConfig
from twindb_backup.configuration.retention import RetentionPolicy


def test_retention(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.retention == RetentionPolicy(
        hourly=24,
        daily=7,
        weekly=4,
        monthly=12,
        yearly=3
    )

    assert tbc.retention_local == RetentionPolicy(
        hourly=24,
        daily=7,
        weekly=4,
        monthly=12,
        yearly=3
    )
