"""GPG configuration"""


class GPGConfig:
    """
    GPG configuration
    """

    def __init__(self, **kwargs):

        for arg in ["recipient", "keyring", "secret_keyring"]:
            setattr(self, "_%s" % arg, kwargs.get(arg, None))

    @property
    def recipient(self):
        """E-mail address of the message recipient."""

        return getattr(self, "_recipient")

    @property
    def keyring(self):
        """Path to keyring."""

        return getattr(self, "_keyring")

    @property
    def secret_keyring(self):
        """Path to secret keyring."""

        return getattr(self, "_secret_keyring")
