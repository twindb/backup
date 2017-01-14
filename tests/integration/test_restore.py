import json
import os
import random
import shlex
from subprocess import call, PIPE, Popen
import pytest

BUCKET = 'twindb-backup-test-%d' % random.randint(0, 1000000)


@pytest.fixture
def foo_bar_dir():
    assert call('rm -rf /foo/bar', shell=True) == 0
    assert call('mkdir -p /foo/bar', shell=True) == 0
    assert call('echo $RANDOM > /foo/bar/file', shell=True) == 0


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


def setup_function():
    cmd = "aws s3 mb s3://%s" % BUCKET
    assert call(shlex.split(cmd)) == 0


def teardown_function():
    cmd = "aws s3 rb --force s3://%s" % BUCKET
    assert call(shlex.split(cmd)) == 0


def test_restore_mysql_inc_creates_logfiles(config_content_mysql_only,
                                            tmpdir):

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

    cmd = ['twindb-backup', '--config', str(config), 'backup', 'hourly']
    assert call(cmd) == 0

    cmd = ['twindb-backup',
           '--config', str(config),
           'status']

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    cout, cerr = proc.communicate()

    status = json.loads(cout)

    key = status['hourly'].keys()[0]
    backup_copy = 's3://' + BUCKET + '/' + key
    dst_dir = str(tmpdir.mkdir('dst'))
    cmd = ['twindb-backup',
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
