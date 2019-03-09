from moto import mock_s3


@mock_s3
def test__create_bucket_creates_the_bucket(s3):
    s3.create_bucket()

    assert s3.s3_client.head_bucket(Bucket='test-bucket')
