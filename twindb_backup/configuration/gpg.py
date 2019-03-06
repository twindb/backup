"""GPG configuration"""


class GPGConfig(object):
    """
    GPG configuration
    """
    def __init__(self, recipient, keyring, **kwargs):

        self._recipient = recipient
        self._keyring = keyring
        self._secret_keyring = kwargs.get('secret_keyring', None)

    @property
    def recipient(self):
        """E-mail address of the message recipient."""

        return self._recipient

    @property
    def keyring(self):
        """Path to keyring."""

        return self._keyring

    @property
    def secret_keyring(self):
        """Path to secret keyring."""

        return self._secret_keyring
