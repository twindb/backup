import os

import pytest


@pytest.fixture
def cache_dir(tmpdir):
    cache_path = tmpdir.mkdir('cache')
    return cache_path


@pytest.fixture
def config_content():
    return """
[source]
backup_dirs=/
backup_mysql=no

[destination]
backup_destination={destination}
keep_local_path=/var/backup/local

[ssh]
ssh_user=root
ssh_key=/root/.ssh/id_rsa
port={port}
backup_host=127.0.0.1
backup_dir=/tmp/backup
"""
