"""SSH destination configuration"""


class SSHConfig(object):
    """SSH destination configuration."""
    def __init__(self,
                 backup_host='127.0.0.1',
                 backup_dir='/var/backup',
                 ssh_user='root',
                 port=22,
                 ssh_key='/root/.ssh/id_rsa'):

        self._host = backup_host
        self._path = backup_dir
        self._user = ssh_user
        self._port = int(port)
        self._key = ssh_key

    @property
    def user(self):
        """SSH user"""
        return self._user

    @property
    def key(self):
        """Path to private SSH key"""
        return self._key

    @property
    def host(self):
        """Hostname or IP address of the SSH destination"""
        return self._host

    @property
    def port(self):
        """SSH port"""
        return self._port

    @property
    def path(self):
        """Remote path to root directory with backups"""
        return self._path
