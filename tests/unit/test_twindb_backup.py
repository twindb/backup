import json

import mock as mock
import os
import pytest

from ConfigParser import ConfigParser
from twindb_backup import delete_local_files, get_directories_to_backup, \
    get_timeout, save_measures
from twindb_backup.backup import run_backup_job
from twindb_backup.source.mysql_source import MySQLSource


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


@pytest.mark.parametrize('keep, calls', [
    (
        1,
        [mock.call('aaa'), mock.call('bbb')]
    ),
    (
        2,
        [mock.call('aaa')]
    ),
    (
        3,
        []
    ),
    (
        0,
        [mock.call('aaa'), mock.call('bbb'), mock.call('ccc')]
    )
])
@mock.patch('twindb_backup.os')
@mock.patch('twindb_backup.glob')
def test_delete_local_files(mock_glob, mock_os, keep, calls):
    mock_glob.glob.return_value = ['aaa', 'bbb', 'ccc']

    delete_local_files('/foo', keep)
    mock_os.unlink.assert_has_calls(calls)


@pytest.mark.parametrize('config_text, dirs', [
    (
        """
[source]
backup_dirs=/etc /root /home
        """,
        ['/etc', '/root', '/home']

    ),
    (
        """
[source]
backup_dirs="/etc /root /home"
        """,
        ['/etc', '/root', '/home']

    ),
    (
        """
[source]
backup_dirs='/etc /root /home'
        """,
        ['/etc', '/root', '/home']

    ),
    (
        """
[source]
        """,
        []

    ),
    (
        """
        """,
        []

    )
])
def test_get_directories_to_backup(config_text, dirs, tmpdir):
    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_text)
    cparser = ConfigParser()
    cparser.read(str(config_file))
    assert get_directories_to_backup(cparser) == dirs


@pytest.mark.parametrize('run_type, timeout', [
    (
        'hourly',
        3600 / 2
    ),
    (
        'daily',
        24 * 3600 / 2
    ),
    (
        'weekly',
        7 * 24 * 3600 / 2
    ),
    (
        'monthly',
        30 * 24 * 3600 / 2
    ),
    (
        'yearly',
        365 * 24 * 3600 / 2
    )
])
def test_get_timeout(run_type, timeout):
    assert get_timeout(run_type) == timeout


@mock.patch('twindb_backup.backup.backup_everything')
@mock.patch('twindb_backup.backup.get_timeout')
def test_run_backup_job_gets_lock(mock_get_timeout, mock_backup_everything,
                                  tmpdir):
    config_content = """
[source]
backup_dirs=/etc /root /home

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=yes
run_yearly=yes
    """
    lock_file = str(tmpdir.join('foo.lock'))
    config_file = tmpdir.join('foo.cfg')
    config_file.write(config_content)

    cparser = ConfigParser()
    cparser.read(str(config_file))

    mock_get_timeout.return_value = 1

    run_backup_job(cparser, 'hourly', lock_file=lock_file)
    mock_backup_everything.assert_called_once_with('hourly', cparser)


@pytest.mark.parametrize('error_log, binlog_coordinate', [
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
        (None, None)
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
        ('mysql-bin.000001', 80960)

    )
])
def test_get_binlog_coordinates(error_log, binlog_coordinate, tmpdir):
    err_log = tmpdir.join('err.log')
    err_log.write(error_log)
    assert MySQLSource.get_binlog_coordinates(str(err_log)) \
        == binlog_coordinate


@pytest.mark.parametrize('error_log,lsn', [
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
        2512733212975
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
        19747438

    )
])
def test_get_lsn(error_log, lsn, tmpdir):
    err_log = tmpdir.join('err.log')
    err_log.write(error_log)
    assert MySQLSource.get_lsn(str(err_log)) == lsn

@pytest.mark.parametrize('data', [
    (
        """
{"measures": [{"duration": 100, "start": 0, "finish": 100}]}
        """
    ),
    (
        """
{"measures": [{"duration": 150, "start": 0, "finish": 150}]}
        """
    ),
    (
        """
{"measures": [{"duration": 444, "start": 444, "finish": 888}]}
        """

    )
])
def test_save_measures_if_log_is_exist(data, tmpdir):
    log_file = tmpdir.join('log')
    log_file.write(data)
    save_measures(50, 100, str(log_file))
    with open(str(log_file)) as data_file:
        data = json.load(data_file)
    last_data=data['measures'][-1]
    assert last_data['duration'] == (last_data['finish'] - last_data['start'])

def test_save_measures_if_log_is_does_not_exist(tmpdir):
    log_file = tmpdir.join('log')
    save_measures(50, 100, str(log_file))
    assert os.path.isfile(str(log_file))
    with open(str(log_file)) as data_file:
        data = json.load(data_file)
    assert len(data['measures']) == 1
    last_data=data['measures'][-1]
    assert last_data['duration'] == (last_data['finish'] - last_data['start'])
