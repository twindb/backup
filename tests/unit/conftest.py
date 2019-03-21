import pytest


@pytest.fixture
def cache_dir(tmpdir):
    cache_path = tmpdir.mkdir('cache')
    return cache_path


@pytest.fixture
def config_content():
    return """
[source]
backup_dirs=/ /root/ /etc "/dir with space/" '/dir foo'
backup_mysql=yes

[destination]
backup_destination={destination}
keep_local_path=/var/backup/local

[s3]
AWS_ACCESS_KEY_ID="XXXXX"
AWS_SECRET_ACCESS_KEY="YYYYY"
AWS_DEFAULT_REGION="us-east-1"
BUCKET="twindb-backups"

[gcs]
GC_CREDENTIALS_FILE="XXXXX"
GC_ENCRYPTION_KEY=
BUCKET="twindb-backups"

[mysql]
mysql_defaults_file=/etc/twindb/my.cnf
expire_log_days = 8

[ssh]
ssh_user="root"
ssh_key=/root/.ssh/id_rsa
port={port}
backup_host='127.0.0.1'
backup_dir=/tmp/backup

[retention]
hourly_copies=24
daily_copies=7
weekly_copies=4
monthly_copies=12
yearly_copies=3
"""
