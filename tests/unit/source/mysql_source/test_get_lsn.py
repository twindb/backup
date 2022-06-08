from textwrap import dedent

import pytest

from twindb_backup.source.mysql_source import MySQLSource


@pytest.mark.parametrize(
    "error_log,lsn",
    [
        (
            dedent(
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
                """
            ),
            2512733212975,
        ),
        (
            dedent(
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
                """
            ),
            19747438,
        ),
        (
            dedent(
                """
                2022-05-25T00:48:25.304583-00:00 0 [Note] [MY-011825] [Xtrabackup] Streaming <STDOUT>
                2022-05-25T00:48:25.304600-00:00 0 [Note] [MY-011825] [Xtrabackup] Done: Streaming file <STDOUT>
                2022-05-25T00:48:25.310742-00:00 0 [Note] [MY-011825] [Xtrabackup] Streaming <STDOUT>
                2022-05-25T00:48:25.310784-00:00 0 [Note] [MY-011825] [Xtrabackup] Done: Streaming file <STDOUT>
                2022-05-25T00:48:25.310819-00:00 0 [Note] [MY-011825] [Xtrabackup] Executing FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS...
                2022-05-25T00:48:25.313658-00:00 0 [Note] [MY-011825] [Xtrabackup] The latest check point (for incremental): '18178282'
                2022-05-25T00:48:25.313709-00:00 0 [Note] [MY-011825] [Xtrabackup] Stopping log copying thread at LSN 18178282
                2022-05-25T00:48:25.314064-00:00 1 [Note] [MY-011825] [Xtrabackup] Starting to parse redo log at lsn = 18178060
                2022-05-25T00:48:25.315639-00:00 0 [Note] [MY-011825] [Xtrabackup] Executing UNLOCK INSTANCE
                2022-05-25T00:48:25.315963-00:00 0 [Note] [MY-011825] [Xtrabackup] All tables unlocked
                2022-05-25T00:48:25.316082-00:00 0 [Note] [MY-011825] [Xtrabackup] Streaming ib_buffer_pool to <STDOUT>
                2022-05-25T00:48:25.316111-00:00 0 [Note] [MY-011825] [Xtrabackup] Done: Streaming ib_buffer_pool to <STDOUT>
                2022-05-25T00:48:25.316647-00:00 0 [Note] [MY-011825] [Xtrabackup] Backup created in directory '/'
                2022-05-25T00:48:25.316733-00:00 0 [Note] [MY-011825] [Xtrabackup] MySQL binlog position: filename 'mysql-bin.000003', position '157'
                2022-05-25T00:48:25.316810-00:00 0 [Note] [MY-011825] [Xtrabackup] Streaming <STDOUT>
                2022-05-25T00:48:25.316826-00:00 0 [Note] [MY-011825] [Xtrabackup] Done: Streaming file <STDOUT>
                2022-05-25T00:48:25.322406-00:00 0 [Note] [MY-011825] [Xtrabackup] Streaming <STDOUT>
                2022-05-25T00:48:25.322445-00:00 0 [Note] [MY-011825] [Xtrabackup] Done: Streaming file <STDOUT>
                2022-05-25T00:48:26.324614-00:00 0 [Note] [MY-011825] [Xtrabackup] Transaction log of lsn (18178282) to (18178292) was copied.
                2022-05-25T00:48:26.546555-00:00 0 [Note] [MY-011825] [Xtrabackup] completed OK!
                """
            ),
            18178282,
        ),
        (
            dedent(
                """
                [00] 2022-06-05 14:14:09 Finished backing up non-InnoDB tables and files
                [01] 2022-06-05 14:14:09 Streaming ./aria_log_control to <STDOUT>
                [01] 2022-06-05 14:14:09         ...done
                [01] 2022-06-05 14:14:09 Streaming ./aria_log.00000001 to <STDOUT>
                [01] 2022-06-05 14:14:09         ...done
                [00] 2022-06-05 14:14:09 Waiting for log copy thread to read lsn 1625461
                [00] 2022-06-05 14:14:10 >> log scanned up to (1625461)
                [00] 2022-06-05 14:14:10 Streaming xtrabackup_binlog_info
                [00] 2022-06-05 14:14:10         ...done
                [00] 2022-06-05 14:14:10 Executing FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS...
                [00] 2022-06-05 14:14:10 mariabackup: The latest check point (for incremental): '1625452'
                mariabackup: Stopping log copying thread.[00] 2022-06-05 14:14:10 >> log scanned up to (1625461)

                [00] 2022-06-05 14:14:10 >> log scanned up to (1625461)
                [00] 2022-06-05 14:14:10 Executing UNLOCK TABLES
                [00] 2022-06-05 14:14:10 All tables unlocked
                [00] 2022-06-05 14:14:10 Streaming ib_buffer_pool to <STDOUT>
                [00] 2022-06-05 14:14:10         ...done
                [00] 2022-06-05 14:14:10 Backup created in directory '/'
                [00] 2022-06-05 14:14:10 MySQL binlog position: filename 'mysql-bin.000002', position '1337', GTID of the last change '0-100-50'
                [00] 2022-06-05 14:14:10 Streaming backup-my.cnf
                [00] 2022-06-05 14:14:10         ...done
                [00] 2022-06-05 14:14:10 Streaming xtrabackup_info
                [00] 2022-06-05 14:14:10         ...done
                [00] 2022-06-05 14:14:10 Redo log (from LSN 1625452 to 1625461) was copied.
                [00] 2022-06-05 14:14:11 completed OK!
                """
            ),
            1625452,
        ),
    ],
)
def test_get_lsn(error_log, lsn, tmpdir):
    err_log = tmpdir.join("err.log")
    err_log.write(error_log)
    # noinspection PyProtectedMember
    assert MySQLSource._get_lsn(str(err_log)) == lsn
