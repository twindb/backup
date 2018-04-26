
from twindb_backup.copy.mysql_copy import MySQLCopy
from twindb_backup.status.mysql_status import MySQLStatus


def test_add(status_raw_empty, tmpdir):
    status = MySQLStatus(status_raw_empty)
    assert status.valid
    mycnf_1 = tmpdir.join('my-1.cnf')
    mycnf_1.write('some_content_1')
    mycnf_2 = tmpdir.join('my-2.cnf')
    mycnf_2.write('some_content_2')

    backup_copy = MySQLCopy(
        'master1', 'daily', 'foo.txt',
        binlog='binlog1',
        position=101,
        type='full',
        lsn=1230,
        backup_started=123,
        backup_finished=456,
        config_files=[str(mycnf_1), str(mycnf_2)]
    )
    status.add(backup_copy)
    assert len(status.daily) == 1
    assert status.daily[backup_copy.key].binlog == 'binlog1'
    assert status.daily[backup_copy.key].position == 101
    assert status.daily[backup_copy.key].type == 'full'
    assert status.daily[backup_copy.key].lsn == 1230
    assert status.daily[backup_copy.key].backup_started == 123
    assert status.daily[backup_copy.key].backup_finished == 456
    assert status.daily[backup_copy.key].duration == 333
    assert {
        str(mycnf_1): 'some_content_1'
    } in status.daily[backup_copy.key].config
    assert {
               str(mycnf_2): 'some_content_2'
           } in status.daily[backup_copy.key].config
