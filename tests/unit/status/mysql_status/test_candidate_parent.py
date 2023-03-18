from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.mysql_status import MySQLStatus


def test_add(status_raw_empty, tmpdir):
    status = MySQLStatus(status_raw_empty)

    status.add(
        MySQLCopy(
            "master1",
            "daily",
            "foo-1.txt",
            binlog="binlog1",
            position=101,
            type="full",
            lsn=1230,
            backup_started=123,
            backup_finished=456,
        )
    )
    status.add(
        MySQLCopy(
            "master1",
            "daily",
            "foo-2.txt",
            binlog="binlog1",
            position=101,
            type="full",
            lsn=1231,
            backup_started=789,
            backup_finished=1000,
        )
    )
    assert status.candidate_parent("hourly").lsn == 1231
