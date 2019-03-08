from ConfigParser import ConfigParser

import mock

from twindb_backup.backup import run_backup_job
from twindb_backup.configuration import TwinDBBackupConfig


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

    cfg = TwinDBBackupConfig(
        config_file=str(config_file)
    )

    mock_get_timeout.return_value = 1

    run_backup_job(cfg, 'hourly', lock_file=lock_file)
    mock_backup_everything.assert_called_once_with(
        'hourly',
        cfg,
        binlogs_only=False
    )
