import mock
from botocore.exceptions import ClientError


def test_get_file_content(s3):
    mock_body = mock.Mock()
    s3.s3_client = mock.Mock()
    s3.s3_client.get_object.return_value = {
        'Body': mock_body
    }
    s3.validate_client_response = mock.Mock()

    # noinspection PyProtectedMember
    s3._get_file_content('foo')
    s3.s3_client.get_object.assert_called_once_with(
        Bucket='test-bucket',
        Key='foo'
    )
    s3.validate_client_response.assert_called_once_with({
        'Body': mock_body
    })
    mock_body.read.assert_called_once_with()


@mock.patch('twindb_backup.destination.s3.time.sleep')
def test_get_file_content_retry(mock_sleep, s3):

    mock_body = mock.Mock()
    s3.s3_client = mock.Mock()

    mock_error_response = {
        'ResponseMetadata': {
            'MaxAttemptsReached': True
        }
    }
    s3.s3_client.get_object.side_effect = [
        ClientError(mock_error_response, 'GetObject'),
        ClientError(mock_error_response, 'GetObject'),
        {
            'Body': mock_body
        }
    ]
    s3.validate_client_response = mock.Mock()

    # noinspection PyProtectedMember
    s3._get_file_content('foo')
    assert s3.s3_client.get_object.call_count == 3
