import pytest


@pytest.fixture
def innobackupex_error_log():
    return """
161122 03:08:50 Executing FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS...
xtrabackup: The latest check point (for incremental): '19747438'
xtrabackup: Stopping log copying thread.
.161122 03:08:50 >> log scanned up to (19747446)

161122 03:08:50 Executing UNLOCK BINLOG
161122 03:08:50 Executing UNLOCK TABLES
161122 03:08:50 All tables unlocked
161122 03:08:50 Backup created in directory '/twindb_backup/.'
MySQL binlog position: filename 'mysql-bin.000001', position '80960', GTID of the last change '2e8afc7a-af69-11e6-8aaf-080027f6b007:1-178'
161122 03:08:50 [00] Streaming backup-my.cnf
161122 03:08:50 [00]        ...done
161122 03:08:50 [00] Streaming xtrabackup_info
161122 03:08:50 [00]        ...done
xtrabackup: Transaction log of lsn (19747438) to (19747446) was copied.
161122 03:08:50 completed OK!    """
