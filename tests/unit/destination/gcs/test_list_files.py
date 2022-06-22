import mock
import pytest

from twindb_backup.destination.gcs import GCS


def setup_mock_bucket(keys):
    mock_bucket = mock.Mock()
    return_value = []
    for i in keys:
        m = mock.Mock()
        m.name = i
        return_value.append(m)
    mock_bucket.list_blobs.return_value = return_value
    return mock_bucket


@pytest.mark.parametrize(
    "gcs_response, expected_list",
    [
        (
            ["object_0", "object_1", "object_2"],
            [
                "gs://test-bucket/object_0",
                "gs://test-bucket/object_1",
                "gs://test-bucket/object_2",
            ],
        ),
        (
            [
                "object_0/part-0000000000000000",
                "object_0/part-0000000000000001",
                "object_0/part-0000000000000002",
                "object_0/part-0000000000000003",
                "object_1/part-0000000000000000",
                "object_1/part-0000000000000001",
                "object_1/part-0000000000000002",
                "object_1/part-0000000000000003",
                "object_2",
            ],
            [
                "gs://test-bucket/object_0",
                "gs://test-bucket/object_1",
                "gs://test-bucket/object_2",
            ],
        ),
    ],
)
@mock.patch.object(GCS, "_gcs_client")
def test__list_files_returns_sorted_list_empty_prefix(mock_client, gcs_response, expected_list, gs):

    mock_client.get_bucket.return_value = setup_mock_bucket(gcs_response)
    files_list = gs.list_files(prefix="")
    assert isinstance(files_list, list)
    assert files_list == expected_list


@pytest.mark.parametrize(
    "prefix, keys, result",
    [
        (
            None,
            ["object_1", "object_2", "object_3"],
            [
                "gs://test-bucket/object_1",
                "gs://test-bucket/object_2",
                "gs://test-bucket/object_3",
            ],
        ),
        # (
        #     'foo',
        #     ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        #     [
        #         'gs://test-bucket/foo/object_1',
        #         'gs://test-bucket/foo/object_2'
        #     ]
        # ),
        # (
        #     'gs://test-bucket/foo',
        #     ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        #     [
        #         'gs://test-bucket/foo/object_1',
        #         'gs://test-bucket/foo/object_2'
        #     ]
        # ),
        # (
        #     'gs://test-bucket/foo/',
        #     ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        #     [
        #         'gs://test-bucket/foo/object_1',
        #         'gs://test-bucket/foo/object_2'
        #     ]
        # ),
        # (
        #     'gs://test-bucket',
        #     ['foo/object_1', 'foo/object_2', 'bar/object_3'],
        #     [
        #         'gs://test-bucket/bar/object_3',
        #         'gs://test-bucket/foo/object_1',
        #         'gs://test-bucket/foo/object_2'
        #     ]
        # )
    ],
)
@mock.patch.object(GCS, "_gcs_client")
def test__list_files(mock_client, prefix, keys, result, gs):

    mock_client.get_bucket.return_value = setup_mock_bucket(keys)

    files_list = gs.list_files(prefix=prefix)
    assert result == files_list


@pytest.mark.parametrize(
    "given_prefix, expected_prefix",
    [
        ("foo", "foo"),
        ("gs://test-bucket/foo", "foo"),
        ("gs://test-bucket/foo/", "foo/"),
        ("gs://test-bucket", None),
    ],
)
@mock.patch.object(GCS, "_gcs_client")
def test__list_files_with_prefix(mock_client, given_prefix, expected_prefix, gs):

    mock_bucket = setup_mock_bucket([])
    mock_client.get_bucket.return_value = mock_bucket

    gs.list_files(prefix=given_prefix)
    mock_bucket.list_blobs.assert_called_once_with(prefix=expected_prefix)
