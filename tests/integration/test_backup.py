import StringIO
import json
import os
import socket

from subprocess import call, Popen, PIPE

from twindb_backup import log
from twindb_backup.destination.s3 import S3


def test__take_file_backup(s3_client, config_content_files_only, tmpdir,
                           foo_bar_dir):
    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_files_only.format(
        TEST_DIR=foo_bar_dir,
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=s3_client.bucket
    )
    config.write(content)

    backup_dir = foo_bar_dir

    # write some content to the directory
    with open(os.path.join(backup_dir, 'file'), 'w') as f:
        f.write("Hello world.")

    hostname = socket.gethostname()
    s3_backup_path = 's3://%s/%s/hourly/files/%s' % \
                     (s3_client.bucket, hostname, backup_dir.replace('/', '_'))

    cmd = ['twindb-backup', '--debug',
           '--config', str(config),
           'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup', '--debug',
           '--config', str(config),
           'ls']
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    log.debug('STDOUT: %s' % out)
    log.debug('STDERR: %s' % err)

    assert proc.returncode == 0

    assert s3_backup_path in out

    backup_to_restore = None
    for line in StringIO.StringIO(out):
        if line.startswith(s3_backup_path):
            backup_to_restore = line.strip()
            break

    dest_dir = tmpdir.mkdir("dst")
    cmd = ['twindb-backup', '--debug',
           '--config', str(config),
           'restore', 'file', '--dst', str(dest_dir), backup_to_restore]

    assert call(cmd) == 0

    path_to_file_restored = '%s/%s/file' % (str(dest_dir), backup_dir)
    assert os.path.exists(path_to_file_restored)

    # And content is same
    path_to_file_orig = "%s/file" % backup_dir
    proc = Popen(['diff', '-Nur',
                  path_to_file_orig,
                  path_to_file_restored],
                 stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    assert not out


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

    log.debug('STDOUT: %s', cout)
    log.debug('STDERR: %s', cerr)

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

    cmd = ['twindb-backup', '--debug', '--config', str(config),
           'backup', 'daily']
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
