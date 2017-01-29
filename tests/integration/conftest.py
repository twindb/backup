import pytest
import os

from subprocess import call
from twindb_backup.destination.s3 import S3

BUCKET = 'twindb-backup-test-travis-%s' % os.environ['TRAVIS_JOB_NUMBER']


@pytest.fixture(scope='session')
def s3_bucket():
    s3_client = S3(BUCKET, os.environ['AWS_ACCESS_KEY_ID'],
                   os.environ['AWS_SECRET_ACCESS_KEY'])
    assert s3_client.create_bucket()

    yield s3_client.bucket

    s3_client.delete_bucket(force=True)


@pytest.fixture
def foo_bar_dir():
    assert call('rm -rf /foo/bar', shell=True) == 0
    assert call('mkdir -p /foo/bar', shell=True) == 0
    assert call('echo $RANDOM > /foo/bar/file', shell=True) == 0


@pytest.fixture
def config_content_files_only(s3_bucket, foo_bar_dir):
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
            BUCKET=s3_bucket
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
