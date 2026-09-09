import pytest

from twindb_backup import LOG, setup_logging

setup_logging(LOG, debug=True)


@pytest.fixture
def cache_dir(tmpdir):
    cache_path = tmpdir.mkdir("cache")
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

[az]
connection_string="DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;EndpointSuffix=core.windows.net"
container_name="twindb-backups"
remote_path="/backups/mysql"
max_concurrency=1

[az.client]
api_version="2019-02-02"
secondary_hostname="ACCOUNT_NAME-secondary.blob.core.windows.net"
max_block_size=4194304
max_single_put_size=67108864
min_large_block_upload_threshold=4194305
use_byte_buffer=true
max_page_size=4194304
max_single_get_size=33554432
max_chunk_get_size=4194304
audience="https://storage.azure.com/"
connection_timeout=20

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
