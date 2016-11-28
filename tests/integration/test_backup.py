import os
import shlex
from subprocess import call, Popen, PIPE
import pytest

BUCKET = 'twindb-backup-test'


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


def setup_module():
    cmd = "aws s3 rm --recursive s3://%s" % BUCKET
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
    assert 's3://twindb-backup-test/master1/hourly/files/_foo_bar' in cout

    copy = None

    for line in cout.split('\n'):
        if line.startswith('s3://twindb-backup-test/master1/hourly/files/_foo_bar'):
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
