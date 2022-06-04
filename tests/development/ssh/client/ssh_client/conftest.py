import pytest

from twindb_backup.ssh.client import SshClient


@pytest.fixture()
def ssh_client():
    return SshClient(
        host="127.0.0.1",
        key="/Users/aleks/src/twindb/backup/vagrant/.vagrant/machines/master1/virtualbox/private_key",
        user="vagrant",
        port=2222,
    )
