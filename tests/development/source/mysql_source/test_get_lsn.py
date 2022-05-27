import pytest

from twindb_backup.source.mysql_source import MySQLSource


@pytest.mark.parametrize(
    "error_log,lsn",
    [
        (
            """
161122 13:37:09 Finished backing up non-InnoDB tables and files
161122 13:37:09 Executing LOCK BINLOG FOR BACKUP...
161122 13:37:09 Executing FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS...
xtrabackup: The latest check point (for incremental): '2512733212975'
xtrabackup: Stopping log copying thread.
.161122 13:37:09 >> log scanned up to (2512734474282)

161122 13:37:09 Executing UNLOCK BINLOG
161122 13:37:09 Executing UNLOCK TABLES
161122 13:37:09 All tables unlocked
161122 13:37:09 [00] Streaming ib_buffer_pool to <STDOUT>
161122 13:37:09 [00]        ...done
161122 13:37:09 Backup created in directory '/root/.'
161122 13:37:09 [00] Streaming backup-my.cnf
161122 13:37:09 [00]        ...done
161122 13:37:09 [00] Streaming xtrabackup_info
161122 13:37:09 [00]        ...done
xtrabackup: Transaction log of lsn (2510405091507) to (2512734474282) was copied.
161122 13:37:09 completed OK!
        """,
            2512733212975,
        ),
        (
            """
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
161122 03:08:50 completed OK!
        """,
            19747438,
        ),
    ],
)
def test_get_lsn(error_log, lsn, tmpdir):
    err_log = tmpdir.join("err.log")
    err_log.write(error_log)
    # noinspection PyProtectedMember
    assert MySQLSource._get_lsn(str(err_log)) == lsn
