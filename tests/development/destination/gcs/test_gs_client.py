import os
from google.cloud import storage


def test_gs_client(creds_file):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_file
    storage_client = storage.Client()
    # storage_client.from_service_account_json()
    bucket_name = 'my-new-bucket'
    bucket = storage_client.create_bucket(bucket_name)

    print('Bucket {} created.'.format(bucket.name))
