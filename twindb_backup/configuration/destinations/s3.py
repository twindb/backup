"""Amazon S3 destrination configuration"""


class S3Config(object):
    """Amazon S3 configuration."""
    def __init__(self,
                 aws_access_key_id,
                 aws_secret_access_key,
                 bucket,
                 aws_default_region='us-east-1'):

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._bucket = bucket
        self._aws_default_region = aws_default_region

    @property
    def aws_access_key_id(self):
        """AWS_ACCESS_KEY_ID"""
        return self._aws_access_key_id

    @property
    def aws_secret_access_key(self):
        """AWS_SECRET_ACCESS_KEY"""
        return self._aws_secret_access_key

    @property
    def bucket(self):
        """S3 bucket"""
        return self._bucket

    @property
    def aws_default_region(self):
        """AWS_DEFAULT_REGION"""
        return self._aws_default_region
