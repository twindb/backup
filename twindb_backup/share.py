from __future__ import print_function

from twindb_backup import TwinDBBackupError
from twindb_backup.configuration import get_destination
from twindb_backup.destination.s3 import S3FileAccess


def share(config, s3_url):
    """
    Function for generate make public file and get public url

    :param config: Config file
    :param s3_url: S3 url to file
    :type s3_url: str
    :raise: TwinDBBackupError
    """
    dst = get_destination(config)
    for run_type in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
        backup_urls = dst.find_files(dst.remote_path, run_type)
        if s3_url in backup_urls:
            dst.set_file_access(S3FileAccess.public_read, s3_url)
            print(dst.get_file_url(s3_url))
            return
    raise TwinDBBackupError("File not found via url: %s", s3_url)
