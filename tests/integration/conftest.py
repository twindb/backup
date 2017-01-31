import pytest
import os
import random

from subprocess import call
from twindb_backup.destination.s3 import S3

BUCKET = 'twindb-backup-test-travis-%s' % \
         os.environ.get('TRAVIS_JOB_NUMBER', random.randint(0, 1000000))


@pytest.fixture(scope='session')
def s3_client():
    client = S3(BUCKET, os.environ['AWS_ACCESS_KEY_ID'],
                os.environ['AWS_SECRET_ACCESS_KEY'])
    assert client.create_bucket()

    yield client

    client.delete_bucket(force=True)


@pytest.fixture
def foo_bar_dir(tmpdir):
    test_dir = tmpdir.join('foo/bar')

    assert call('rm -rf %s' % str(test_dir), shell=True) == 0
    assert call('mkdir -p %s' % str(test_dir), shell=True) == 0
    assert call('echo $RANDOM > %s' %
                str(test_dir.join('file')), shell=True) == 0

    return str(test_dir)


@pytest.fixture
def config_content_files_only(s3_client, foo_bar_dir):
    try:
        return """
[source]
backup_dirs={TEST_DIR}

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
            TEST_DIR=foo_bar_dir,
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket
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
