import json
import os
import shlex
import socket

from subprocess import call, Popen, PIPE
from twindb_backup.destination.s3 import S3


def test__take_file_backup(s3_client, config_content_files_only, tmpdir):
    config = tmpdir.join('twindb-backup.cfg')
    config.write(config_content_files_only)
    cmd = ['twindb-backup',
           '--config', str(config),
           'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup',
           '--config', str(config),
           'ls']
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()
    hostname = socket.gethostname()
    assert 's3://%s/%s/hourly/files/_foo_bar' % (s3_client.bucket, hostname) in cout

    copy = None

    for line in cout.split('\n'):
        pattern = 's3://twindb-backup-test/%s/hourly/files/_foo_bar' \
                  % hostname
        if line.startswith(pattern):
            copy = line
            break

    dstdir = tmpdir.mkdir("dst")

    cmd = ['twindb-backup',
           '--config', str(config),
           'restore', 'file', '--dst', str(dstdir)]

    assert call(cmd) == 0

    proc = Popen(shlex.split('diff -Nur %s %s' % (copy, str(dstdir))),
                 stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()
    assert not cout


def test__take_mysql_backup(s3_client, config_content_mysql_only, tmpdir):

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=s3_client.bucket,
        daily_copies=1,
        hourly_copies=2
    )
    config.write(content)
    cmd = ['twindb-backup',
           '--config', str(config),
           'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup',
           '--config', str(config),
           'status']
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()

    print('STDOUT:', cout)
    print('STDERR:', cerr)

    key = json.loads(cout)['hourly'].keys()[0]

    assert key


def test__take_mysql_backup_retention(s3_client, config_content_mysql_only,
                                      tmpdir):

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=s3_client.bucket,
        daily_copies=1,
        hourly_copies=2
    )
    config.write(content)
    cmd = ['twindb-backup', '--config', str(config), 'backup', 'daily']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--config', str(config), 'backup', 'daily']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--config', str(config), 'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--config', str(config), 'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--config', str(config), 'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup',
           '--config', str(config),
           'status']

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()

    status = json.loads(cout)

    assert len(status['daily'].keys()) == 1
    assert len(status['hourly'].keys()) == 2


def test__s3_find_files_returns_sorted(s3_client, config_content_mysql_only,
                                       tmpdir):
    # cleanup the bucket first
    s3_client.delete_all_objects()

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=s3_client.bucket,
        daily_copies=5,
        hourly_copies=2
    )
    config.write(content)
    cmd = ['twindb-backup', '--config', str(config), 'backup', 'daily']
    n_runs = 3
    for x in xrange(n_runs):
        assert call(cmd) == 0

    dst = S3(s3_client.bucket, os.environ['AWS_ACCESS_KEY_ID'],
             os.environ['AWS_SECRET_ACCESS_KEY'])
    for x in xrange(10):
        result = dst.find_files(dst.remote_path, 'daily')
        assert len(result) == n_runs
        assert result == sorted(result)
        prefix = "{remote_path}/{hostname}/{run_type}/mysql/mysql-".format(
            remote_path=dst.remote_path,
            hostname=socket.gethostname(),
            run_type='daily'
        )
        objects = [f.key for f in dst.list_files(prefix)]
        assert len(objects) == n_runs
        assert objects == sorted(objects)
