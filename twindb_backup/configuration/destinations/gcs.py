"""Google Cloud Storage destrination configuration"""


class GCSConfig(object):
    """Google Cloud Storage configuration."""
    def __init__(self,
                 gc_credentials_file,
                 gc_encryption_key,
                 bucket):

        self._gc_credentials_file = gc_credentials_file
        self._gc_encryption_key = gc_encryption_key
        self._bucket = bucket

    @property
    def gc_credentials_file(self):
        """AWS_ACCESS_KEY_ID"""
        return self._gc_credentials_file

    @property
    def gc_encryption_key(self):
        """AWS_SECRET_ACCESS_KEY"""
        return self._gc_encryption_key

    @property
    def bucket(self):
        """S3 bucket"""
        return self._bucket
