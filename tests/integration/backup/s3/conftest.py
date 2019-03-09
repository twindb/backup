import pytest
import os
import random

from subprocess import call

import time

from twindb_backup import LOG, setup_logging
from twindb_backup.destination.s3 import S3
setup_logging(LOG, debug=True)


@pytest.fixture()
def bucket_name():
    travis_job_number = os.environ.get('TRAVIS_JOB_NUMBER')
    LOG.debug('TRAVIS_JOB_NUMBER=%s' % travis_job_number)

    number = random.randint(0, 1000000)
    LOG.debug('Default job number %d' % number)

    if travis_job_number:
        bucket = 'twindb-backup-test-travis-%s' % travis_job_number
    else:
        bucket = 'twindb-backup-test-travis-%d' % number

    return '%s-%s' % (bucket, time.time())


@pytest.fixture()
def s3_client(bucket_name):
    LOG.debug('Bucket: %s' % bucket_name)
    client = S3(
        bucket=bucket_name,
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )
    assert client.create_bucket()

    yield client

    client.delete_bucket(force=True)


@pytest.fixture
def foo_bar_dir(tmpdir):
    test_dir = tmpdir.join('foo/bar')

    assert call('rm -rf %s' % str(test_dir), shell=True) == 0
    assert call('mkdir -p %s' % str(test_dir), shell=True) == 0
    assert call('echo $RANDOM > %s' %
                str(test_dir.join('file')), shell=True) == 0

    return str(test_dir)


@pytest.fixture
def config_content_files_only():
    return """
[source]
backup_dirs={TEST_DIR}

[destination]
backup_destination=s3

[s3]
AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION=us-east-1
BUCKET={BUCKET}

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=no
run_yearly=yes

[retention]
hourly_copies=2
daily_copies=1
weekly_copies=1
monthly_copies=1
yearly_copies=1

[retention_local]
hourly_copies=1
daily_copies=1
weekly_copies=0
monthly_copies=0
yearly_copies=0
"""


@pytest.fixture
def config_content_mysql_only():
    return """
[source]
backup_mysql=yes

[destination]
backup_destination=s3

[mysql]
mysql_defaults_file={MY_CNF}
full_backup=daily

[s3]
AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION=us-east-1
BUCKET={BUCKET}

[intervals]
run_hourly=yes
run_daily=yes
run_weekly=yes
run_monthly=no
run_yearly=yes

[retention]
hourly_copies={hourly_copies}
daily_copies={daily_copies}
weekly_copies=1
monthly_copies=1
yearly_copies=1

[retention_local]
hourly_copies=1
daily_copies=1
weekly_copies=0
monthly_copies=0
yearly_copies=0
"""


@pytest.fixture
def gpg_public_key():
    key = """
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v2

mI0EWLOi1wEEAOykcV8bgUBAY8itu8Zjl9lUYi1vMimQdE6WwaQ+8w6AKoYXnDPV
NUGRpmoM7PmaMGHCgYydKoZuVZJcEdU+ClOT+9gTa8hstKK/BNLPhiWAw7tbEIhb
WwloTmIOEpVWeGM5+lJ3aELMH5ZH3uQ91iCA14q4TWLI840eXVSdlgSfABEBAAG0
EUZvbyBCYXIgPGZvb0BiYXI+iLkEEwEIACMFAlizotcCGwMHCwkIBwMCAQYVCAIJ
CgsEFgIDAQIeAQIXgAAKCRDQ3Yb3pV7OdQ84A/9jmdlpQDQ+Q3R9noV2HwgQUgjy
NSzeJgJnQufEAThF132R/bAQ6mXJgMq7e8mA4zXX0ZKkND/WvNOocRUxwZrRwHJa
LDXatAkwiexGG6BrHhUtRLk19PXgvvTim50iJinMI3UFGtQjYL7ocb8osTAsVTd9
/1i4wGxqCzEz2TeCe7iNBFizotcBBACpkynGVDaiEcf9xlgOfAgUwRxrDYG5RFFC
MJHVwOAGufbNcCtZ3EaKIU20l4y83Yy82xBEzDK21sIs/NeNgbry54q7Q01FKaGq
6wwSALfWn+VXLk2x8saMj8p8+ChXb53yp0yiSbk3lzfQT91Uo+3Ke57c7pm+0Oz7
OrBtXY1RwwARAQABiJ8EGAEIAAkFAlizotcCGwwACgkQ0N2G96VeznWPPQP+Mr1W
0GyVq4clvCq2BvNA+nBrXFuSvgM5jFz43NkeR3WykP1/QhyduEsXUweejI43Y3gs
OVwe8w+/V1wVQeNptRoyW8rV9pfxgdiuG3xXS7LogGEX3Sy5cMUzrtTLKQM4rk+A
s34qC2jpoQiiGmB0NFNI90epbvMQPK4AtuQOE1c=
=p0uq
-----END PGP PUBLIC KEY BLOCK-----
"""
    return key


@pytest.fixture
def gpg_private_key():
    key = """
-----BEGIN PGP PRIVATE KEY BLOCK-----
Version: GnuPG v2

lQHYBFizotcBBADspHFfG4FAQGPIrbvGY5fZVGItbzIpkHROlsGkPvMOgCqGF5wz
1TVBkaZqDOz5mjBhwoGMnSqGblWSXBHVPgpTk/vYE2vIbLSivwTSz4YlgMO7WxCI
W1sJaE5iDhKVVnhjOfpSd2hCzB+WR97kPdYggNeKuE1iyPONHl1UnZYEnwARAQAB
AAP8D2v8E3Woa7aGijqARUKST9CHAW0AuOK4IbMDdZ/AmU5S9yAsxtf7O4WxcbHb
87xPsN9LMA3CCrbADuS/KMV9SKsDcGAWc8BPHT2ae1uiZ5gqgOMRo/No7v1elMiK
/ntZgb7qsgaDCBdyOcigd9KSWxdynoXPrROhnCsW8/Qc4GECAPIkOKpqXkhTVsRh
swkfXQS7f836mOhfoiJA3INoWFwpPiJS1VZYZ3XLUuuAC012lOP5awr7MoqsQL9+
lcC62k8CAPovpmQ4giHPjUUJzVoLuM5h9VP99WFOkzDcLr4L0U524Pnl4419B5qF
Vujpo7XsAL7e2EOEPozgonYPLHzIrLECANYAbXFylhMC+9xVdbADZxigNgNZD31v
qMtzGmubmlvQUIBeU3p8ogDc8osDKguMvqXCO+WLcB+efDRdtb3NnwufoLQRRm9v
IEJhciA8Zm9vQGJhcj6IuQQTAQgAIwUCWLOi1wIbAwcLCQgHAwIBBhUIAgkKCwQW
AgMBAh4BAheAAAoJENDdhvelXs51DzgD/2OZ2WlAND5DdH2ehXYfCBBSCPI1LN4m
AmdC58QBOEXXfZH9sBDqZcmAyrt7yYDjNdfRkqQ0P9a806hxFTHBmtHAclosNdq0
CTCJ7EYboGseFS1EuTX09eC+9OKbnSImKcwjdQUa1CNgvuhxvyixMCxVN33/WLjA
bGoLMTPZN4J7nQHYBFizotcBBACpkynGVDaiEcf9xlgOfAgUwRxrDYG5RFFCMJHV
wOAGufbNcCtZ3EaKIU20l4y83Yy82xBEzDK21sIs/NeNgbry54q7Q01FKaGq6wwS
ALfWn+VXLk2x8saMj8p8+ChXb53yp0yiSbk3lzfQT91Uo+3Ke57c7pm+0Oz7OrBt
XY1RwwARAQABAAP8DuFqHgxPywMScLOSEJtTvjZ//ujthEt5cfx/H6nQPubcwIRi
WX1Z908a2YkfAYfTjNMQZ2kf3imUWoxJghJrTDbyDK42GnLoO37cgRf3kCYHYafz
vc1X0mR17hNb5Y7Z1+IYiCRNNnwX13Brnq8+UsHxTdkYFmO8ZE5EUp+HtDECAM/M
s+0yWAqhKlVF6CiQIyb9J65bp2raLiZ/pXEUkgafPNWiZQk5A6yarpmAD+0dxyMs
oivWY6Yc7jrphdj8EwkCANDopSve+yR9ypAqUmuOY8QRSTbKkXL5VYpaFhn5gvZD
uAC+CZQgdnOGOlEjChhjCvIPr1NCJ6with98yx/ctWsCALhIRhI6ob/Za35AY4Qd
GlNz2E4rscP4dki0xGUWtwfoVqG8fsGr5DxFAa/ihcJtl7u1xneYaF4a8Q0jftT3
E9CYfYifBBgBCAAJBQJYs6LXAhsMAAoJENDdhvelXs51jz0D/jK9VtBslauHJbwq
tgbzQPpwa1xbkr4DOYxc+NzZHkd1spD9f0IcnbhLF1MHnoyON2N4LDlcHvMPv1dc
FUHjabUaMlvK1faX8YHYrht8V0uy6IBhF90suXDFM67UyykDOK5PgLN+Kgto6aEI
ohpgdDRTSPdHqW7zEDyuALbkDhNX
=DjLy
-----END PGP PRIVATE KEY BLOCK-----"""
    return key


@pytest.fixture
def config_content_mysql_aenc(config_content_mysql_only):

    return config_content_mysql_only + """

[gpg]
keyring = {gpg_keyring}
secret_keyring = {gpg_secret_keyring}
recipient = foo@bar
"""


@pytest.fixture
def config_content_files_aenc(config_content_files_only):
    return config_content_files_only + """

[gpg]
keyring = {gpg_keyring}
secret_keyring = {gpg_secret_keyring}
recipient = foo@bar
"""
