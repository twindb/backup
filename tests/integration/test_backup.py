import json
import os
import random
import shlex
import socket
from subprocess import call, Popen, PIPE
import pytest

BUCKET = 'twindb-backup-test-%d' % random.randint(0, 1000000)


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


def setup_module():
    cmd = "aws s3 mb s3://%s" % BUCKET
    assert call(shlex.split(cmd)) == 0


def teardown_module():
    cmd = "aws s3 rb --force s3://%s" % BUCKET
    assert call(shlex.split(cmd)) == 0


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
    config.write(config_content_mysql_only)
    cmd = ['twindb-backup',
           '--config', str(config),
           'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup',
           '--config', str(config),
           'status']
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()

    key = json.loads(cout)['hourly'].keys()[0]

    assert key


def test_take_mysql_backup_retention(config_content_mysql_only, tmpdir):

    config = tmpdir.join('twindb-backup.cfg')
    config.write(config_content_mysql_only)
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
