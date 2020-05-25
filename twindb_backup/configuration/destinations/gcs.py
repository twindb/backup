"""Google Cloud Storage destination configuration."""


class GCSConfig:
    """Google Cloud Storage configuration."""

    __attr__ = ["gc_credentials_file", "gc_encryption_key", "bucket"]

    def __init__(self, **kwargs):

        for opt in self.__attr__:
            setattr(self, "_%s" % opt, kwargs.get(opt))

    @property
    def gc_credentials_file(self):
        """GC_CREDENTIALS_FILE"""
        return getattr(self, "_gc_credentials_file")

    @property
    def gc_encryption_key(self):
        """GC_ENCRYPTION_KEY"""
        return getattr(self, "_gc_encryption_key")

    @property
    def bucket(self):
        """GCS bucket"""
        return getattr(self, "_bucket")
