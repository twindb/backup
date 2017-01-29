import json
import os
import pytest
import shlex
import socket

from subprocess import call, Popen, PIPE
from twindb_backup.destination.s3 import S3

BUCKET = 'twindb-backup-test-travis-%s' % os.environ['TRAVIS_JOB_NUMBER']


@pytest.fixture
def foo_bar_dir():
    assert call('rm -rf /foo/bar', shell=True) == 0
    assert call('mkdir -p /foo/bar', shell=True) == 0
    assert call('echo $RANDOM > /foo/bar/file', shell=True) == 0


@pytest.fixture
def config_content_files_only(foo_bar_dir):
    try:
        return """
[source]
backup_dirs=/foo/bar

[destination]
backup_destination=s3

[s3]
AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION=us-east-1
BUCKET={BUCKET}

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=no
run_yearly=yes

[retention]
hourly_copies=2
daily_copies=1
weekly_copies=1
monthly_copies=1
yearly_copies=1

[retention_local]
hourly_copies=1
daily_copies=1
weekly_copies=0
monthly_copies=0
yearly_copies=0
    """.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=BUCKET
        )
    except KeyError as err:
        print('Environment variable %s must be defined' % err)
        exit(1)


@pytest.fixture
def config_content_mysql_only():
    try:
        f = open('/root/.my.cnf', 'w')
        f.write("""
[client]
user=root
password=
""")

        return """
[source]
backup_mysql=yes

[destination]
backup_destination=s3

[mysql]
mysql_defaults_file=/root/.my.cnf
full_backup=daily

[s3]
AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION=us-east-1
BUCKET={BUCKET}

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=no
run_yearly=yes

[retention]
hourly_copies={hourly_copies}
daily_copies={daily_copies}
weekly_copies=1
monthly_copies=1
yearly_copies=1

[retention_local]
hourly_copies=1
daily_copies=1
weekly_copies=0
monthly_copies=0
yearly_copies=0
    """
    except KeyError as err:
        print('Environment variable %s must be defined' % err)
        exit(1)

# THis is the s3 client that is used in remainder of the tests.
s3_client = None


def setup_function():
    global s3_client

    s3_client = S3(BUCKET, os.environ['AWS_ACCESS_KEY_ID'],
                   os.environ['AWS_SECRET_ACCESS_KEY'])
    assert s3_client.create_bucket()


def teardown_function():
    global s3_client

    if s3_client:
        assert s3_client.delete_bucket(force=True)


def test_take_file_backup(config_content_files_only, tmpdir):
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
    assert 's3://%s/%s/hourly/files/_foo_bar' % (BUCKET, hostname) in cout

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


def test_take_mysql_backup(config_content_mysql_only, tmpdir):

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=BUCKET,
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


def test_take_mysql_backup_retention(config_content_mysql_only, tmpdir):

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=BUCKET,
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


def test_s3_find_files_returns_sorted(config_content_mysql_only, tmpdir):

    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_mysql_only.format(
        AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
        AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
        BUCKET=BUCKET,
        daily_copies=5,
        hourly_copies=2
    )
    config.write(content)
    cmd = ['twindb-backup', '--config', str(config), 'backup', 'daily']
    n_runs = 3
    for x in xrange(n_runs):
        assert call(cmd) == 0

    dst = S3(BUCKET, os.environ['AWS_ACCESS_KEY_ID'],
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
