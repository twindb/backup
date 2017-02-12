import pytest
import os
import random

from subprocess import call

from twindb_backup import LOG, setup_logging
from twindb_backup.destination.s3 import S3

setup_logging(LOG, debug=True)


@pytest.fixture(scope='session')
def bucket_name():
    travis_job_number = os.environ.get('TRAVIS_JOB_NUMBER')
    LOG.debug('TRAVIS_JOB_NUMBER=%s' % travis_job_number)

    number = random.randint(0, 1000000)
    LOG.debug('Default job number %d' % number)

    if travis_job_number:
        bucket = 'twindb-backup-test-travis-%s' % travis_job_number
    else:
        bucket = 'twindb-backup-test-travis-%d' % number

    return bucket


@pytest.fixture(scope='session')
def s3_client(bucket_name):
    LOG.debug('Bucket: %s' % bucket_name)
    client = S3(bucket_name, os.environ['AWS_ACCESS_KEY_ID'],
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
def config_content_files_only():
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
"""


@pytest.fixture
def config_content_mysql_only(tmpdir):
    contents = """
[client]
user=root
password=
"""
    if os.path.exists(os.path.expanduser("~/.my.cnf")):
        with open(os.path.expanduser("~/.my.cnf")) as my_cnf:
            contents = my_cnf.read()

    f = tmpdir.join('.my.cnf')
    f.write(contents)

    return """
[source]
backup_mysql=yes

[destination]
backup_destination=s3

[mysql]
mysql_defaults_file=%s
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
""" % str(f)
