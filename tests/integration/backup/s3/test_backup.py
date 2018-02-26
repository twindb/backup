import StringIO
import json
import os

from tests.integration.conftest import docker_execute, get_twindb_config_dir
from twindb_backup.destination.s3 import S3, AWSAuthOptions


def test__take_file_backup(master1,
                           docker_client,
                           s3_client,
                           config_content_files_only):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'

    backup_dir = "/etc/twindb"

    with open(twindb_config_host, 'w') as fp:
        content = config_content_files_only.format(
            TEST_DIR=backup_dir,
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    # Check that backup copy is in "twindb-backup ls" output
    hostname = 'master1_1'
    s3_backup_path = 's3://%s/%s/hourly/files/%s' % (
        s3_client.bucket,
        hostname,
        backup_dir.replace('/', '_')
    )
    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'ls']

    ret, cout = docker_execute(docker_client, master1['Id'], cmd)

    print(cout)
    assert ret == 0

    assert s3_backup_path in cout

    backup_to_restore = None
    for line in StringIO.StringIO(cout):
        if line.startswith(s3_backup_path):
            backup_to_restore = line.strip()
            break

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'restore', 'file', '--dst', '/tmp/restore', backup_to_restore]

    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    # Check that restored file exists
    path_to_file_restored = '/tmp/restore/etc/twindb/twindb-backup-1.cfg'
    cmd = ['ls', path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    # And content is same
    cmd = ['diff',
           '/tmp/restore/etc/twindb/twindb-backup-1.cfg',
           twindb_config_guest]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    # empty output
    assert not cout
    # zero exit code if no differences
    assert ret == 0


def test__take_mysql_backup(master1,
                            docker_client,
                            s3_client,
                            config_content_mysql_only):

    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_only.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup',
           '--debug',
           '--config', twindb_config_guest,
           'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup',
           '--config', twindb_config_guest,
           'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)

    key = json.loads(cout)['hourly'].keys()[0]

    assert key.endswith('.xbstream.gz')


def test__take_mysql_backup_retention(master1,
                                      docker_client,
                                      s3_client,
                                      config_content_mysql_only):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_only.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'backup', 'daily']
    for i in range(0, 3):
        ret, cout = docker_execute(docker_client, master1['Id'], cmd)
        print(cout)
        assert ret == 0

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'backup', 'hourly']
    for i in range(0, 3):
        ret, cout = docker_execute(docker_client, master1['Id'], cmd)
        print(cout)
        assert ret == 0

    cmd = ['twindb-backup', '--config', twindb_config_guest, 'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    print(cout)
    status = json.loads(cout)

    assert len(status['daily'].keys()) == 1
    assert len(status['hourly'].keys()) == 2


def test__s3_find_files_returns_sorted(master1,
                                       docker_client,
                                       s3_client,
                                       config_content_mysql_only):
    # cleanup the bucket first
    s3_client.delete_all_objects()

    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)
    backup_dir = "/etc/twindb"

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_only.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=5,
            hourly_copies=2,
            TEST_DIR=backup_dir,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'backup', 'daily']

    n_runs = 3
    for x in xrange(n_runs):
        ret, cout = docker_execute(docker_client, master1['Id'], cmd)
        print(cout)
        assert ret == 0
    hostname = 'master1_1'
    dst = S3(s3_client.bucket,
             AWSAuthOptions(os.environ['AWS_ACCESS_KEY_ID'],
                            os.environ['AWS_SECRET_ACCESS_KEY'])
             )

    for x in xrange(10):
        result = dst.find_files(dst.remote_path, 'daily')
        assert len(result) == n_runs
        assert result == sorted(result)
        prefix = "{remote_path}/{hostname}/{run_type}/mysql/mysql-".format(
            remote_path=dst.remote_path,
            hostname=hostname,
            run_type='daily'
        )
        files = dst.list_files(prefix)
        assert len(files) == n_runs
        assert files == sorted(files)


def test__take_mysql_backup_aenc_suffix_gpg(master1,
                                            docker_client,
                                            s3_client,
                                            config_content_mysql_aenc):

    # cleanup the bucket first
    s3_client.delete_all_objects()

    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=7,
            hourly_copies=3,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--config', twindb_config_guest, 'backup', 'daily']

    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--config', twindb_config_guest, 'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    print(cout)
    key = json.loads(cout)['daily'].keys()[0]

    assert key.endswith(".xbstream.gz.gpg")


def test__take_file_backup_with_aenc(master1,
                                     docker_client,
                                     s3_client,
                                     config_content_mysql_aenc):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)
    backup_dir = "/etc/twindb"
    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=7,
            hourly_copies=3,
            TEST_DIR=backup_dir,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)
    hostname = 'master1_1'
    s3_backup_path = 's3://%s/%s/hourly/files/%s' % \
                     (s3_client.bucket, hostname, backup_dir.replace('/', '_'))
    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'ls']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    assert s3_backup_path in cout
    backup_to_restore = None
    for line in StringIO.StringIO(cout):
        if line.startswith(s3_backup_path):
            backup_to_restore = line.strip()
            break

    cmd = ['mkdir', '-p', '/tmp/dst_file_enc']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'restore', 'file', '--dst', '/tmp/dst_file_enc', backup_to_restore]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    path_to_file_restored = '/tmp/dst_file_enc/%s/file' % (backup_dir)
    assert os.path.exists(path_to_file_restored)
    # And content is same
    path_to_file_orig = "%s/file" % backup_dir
    cmd = ['diff', '-Nur', path_to_file_orig, path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    assert not cout


def test__take_mysql_backup_aenc_restores_full(master1,
                                               docker_client,
                                               s3_client,
                                               config_content_mysql_aenc):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=7,
            hourly_copies=3,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--config', twindb_config_guest, 'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup',
           '--config', twindb_config_guest,
           'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    key = json.loads(cout)['daily'].keys()[0]
    backup_copy = 's3://' + s3_client.bucket + '/' + key

    cmd = ['mkdir', '-p', '/tmp/dst']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'restore', 'mysql',
           backup_copy,
           '--dst', '/tmp/dst']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/ibdata1']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/ib_logfile0']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/ib_logfile1']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/mysql/user.MYD']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/backup-my.cnf']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/xtrabackup_logfile']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst/_config/etc/my.cnf', '||', 'test', '-f', '/tmp/dst/_config/etc/mysql/my.cnf']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0

def test__take_mysql_backup_aenc_restores_inc(master1,
                                              docker_client,
                                              s3_client,
                                              config_content_mysql_aenc):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    contents = """
[client]
user=dba
password=qwerty
"""

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'],
            BUCKET=s3_client.bucket,
            daily_copies=7,
            hourly_copies=3,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--config', twindb_config_guest, 'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['twindb-backup', '--config', twindb_config_guest, 'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['twindb-backup', '--config', twindb_config_guest, 'status']
    print(cout)
    assert ret == 0
    key = json.loads(cout)['hourly'].keys()[0]
    backup_copy = 's3://' + s3_client.bucket + '/' + key

    cmd = ['mkdir', '-p', '/tmp/dst_full']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'restore', 'mysql',
           backup_copy,
           '--dst', '/tmp/dst_full']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/ibdata1']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/ib_logfile0']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/ib_logfile1']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/mysql/user.MYD']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/backup-my.cnf']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/xtrabackup_logfile']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full/_config/etc/my.cnf', '||', 'test', '-f', '/tmp/dst_full/_config/etc/mysql/my.cnf']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    assert ret == 0
