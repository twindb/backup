import json
import os

from subprocess import call, PIPE, Popen


def test__restore_mysql_inc_creates_log_files(s3_client, tmpdir,
                                              config_content_mysql_only):

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=s3_client.bucket,
        daily_copies=1,
        hourly_copies=2
    )
    config.write(content)
    cmd = ['twindb-backup', '--debug', '--config', str(config), 'backup', 'daily']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--debug', '--config', str(config), 'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--debug',
           '--config', str(config),
           'status']

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()

    status = json.loads(cout)

    key = status['hourly'].keys()[0]
    backup_copy = 's3://' + s3_client.bucket + '/' + key
    dst_dir = str(tmpdir.mkdir('dst'))
    cmd = ['twindb-backup', '--debug',
           '--config', str(config),
           'restore', 'mysql',
           backup_copy,
           '--dst', dst_dir]
    assert call(cmd) == 0
    call(['find', dst_dir])
    assert os.path.exists(dst_dir + '/ibdata1')
    assert os.path.exists(dst_dir + '/ib_logfile0')
    assert os.path.exists(dst_dir + '/ib_logfile1')
    assert os.path.exists(dst_dir + '/mysql/user.MYD')
    assert os.path.exists(dst_dir + '/backup-my.cnf')
    assert os.path.exists(dst_dir + '/xtrabackup_logfile')
    assert os.path.exists(dst_dir + '/_config/etc/my.cnf') or \
        os.path.exists(dst_dir + '/_config/etc/mysql/my.cnf')
