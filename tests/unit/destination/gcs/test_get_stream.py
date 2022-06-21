import mock

from twindb_backup.destination.gcs import GCS


@mock.patch("twindb_backup.destination.gcs.Process")
@mock.patch("twindb_backup.destination.gcs.os")
def test_get_stream_calls_popen(mock_os, mock_process):
    mock_os.pipe.return_value = (100, 200)
    gs = GCS(gc_credentials_file="foo", bucket="bar")
    mock_copy = mock.Mock()
    mock_copy.key = "foo-key"

    with gs.get_stream(mock_copy):
        pass

    mock_process.assert_called_once_with(target=gs._download_to_pipe, args=("foo-key", 100, 200))
