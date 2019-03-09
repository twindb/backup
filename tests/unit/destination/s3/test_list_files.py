import pytest
from moto import mock_s3


@mock_s3
def test__list_files_returns_sorted_list_empty_prefix(s3):
    s3.create_bucket()

    s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key='object_1'
    )
    s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key='object_3'
    )
    s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key='object_2'
    )

    files_list = s3.list_files(prefix='')
    assert files_list == [
        's3://test-bucket/object_1',
        's3://test-bucket/object_2',
        's3://test-bucket/object_3'
    ]


@pytest.mark.parametrize('prefix, keys, result', [
    (
        '',
        ['object_1', 'object_2', 'object_3'],
        [
            's3://test-bucket/object_1',
            's3://test-bucket/object_2',
            's3://test-bucket/object_3'
        ]
    ),
    (
        'foo',
        ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        [
            's3://test-bucket/foo/object_1',
            's3://test-bucket/foo/object_2'
        ]
    ),
    (
        's3://test-bucket/foo',
        ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        [
            's3://test-bucket/foo/object_1',
            's3://test-bucket/foo/object_2'
        ]
    ),
    (
        's3://test-bucket/foo/',
        ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        [
            's3://test-bucket/foo/object_1',
            's3://test-bucket/foo/object_2'
        ]
    ),
    (
        's3://test-bucket',
        ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        [
            's3://test-bucket/bar/object_3',
            's3://test-bucket/foo/object_1',
            's3://test-bucket/foo/object_2'
        ]
    )
])
@mock_s3
def test__list_files(prefix, keys, result, s3):
    s3.create_bucket()

    for key in keys:
        s3.s3_client.put_object(
            Body='hello world',
            Bucket='test-bucket',
            Key=key
        )

    files_list = s3.list_files(prefix=prefix)
    assert files_list == result


@mock_s3
def test__list_files_with_pattern(s3):
    s3.create_bucket()

    s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key='object_1'
    )
    s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key='object/foo/bar'
    )
    s3.s3_client.put_object(
        Body='hello world',
        Bucket='test-bucket',
        Key='object_2'
    )

    files_list = s3.list_files(prefix='', pattern='_2')
    assert files_list == ['s3://test-bucket/object_2']

    files_list = s3.list_files(prefix='', pattern='.*/foo/.*')
    assert files_list == ['s3://test-bucket/object/foo/bar']
