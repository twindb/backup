import StringIO
import json
import os
import magic

from tests.integration.conftest import docker_execute, get_twindb_config_dir
from twindb_backup.destination.gcs import GCS, GCAuthOptions


def test__take_file_backup(master1,
                           docker_client,
                           gcs_client,
                           config_content_files_only):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'

    backup_dir = "/etc/twindb"

    with open(twindb_config_host, 'w') as fp:
        content = config_content_files_only.format(
            TEST_DIR=backup_dir,
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket
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
    gcs_backup_path = 'gs://%s/%s/hourly/files/%s' % (
        gcs_client.bucket,
        hostname,
        backup_dir.replace('/', '_')
    )
    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'ls']

    ret, cout = docker_execute(docker_client, master1['Id'], cmd)

    print(cout)
    assert ret == 0

    assert gcs_backup_path in cout

    backup_to_restore = None
    for line in StringIO.StringIO(cout):
        if line.startswith(gcs_backup_path):
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
                            gcs_client,
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
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
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
    print(cout)
    key = json.loads(cout)['hourly'].keys()[0]

    assert key.endswith('.xbstream.gz')


def test__take_mysql_backup_retention(master1,
                                      docker_client,
                                      gcs_client,
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
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest,
           'backup', 'daily']
    for i in range(0, 3):
        ret, cout = docker_execute(docker_client, master1['Id'], cmd)
        print(cout)
        assert ret == 0

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest,
           'backup', 'hourly']
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


def test__gcs_find_files_returns_sorted(master1,
                                        docker_client,
                                        gcs_client,
                                        config_content_mysql_only):
    # cleanup the bucket first
    gcs_client.delete_all_objects()

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
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
            daily_copies=5,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest,
           'backup', 'daily']

    n_runs = 3
    for x in xrange(n_runs):
        ret, cout = docker_execute(docker_client, master1['Id'], cmd)
        print(cout)
        assert ret == 0
    hostname = 'master1_1'
    dst = GCS(gcs_client.bucket,
              GCAuthOptions(os.environ['GC_CREDENTIALS_FILE']))

    for x in xrange(10):
        result = dst.list_files(dst.remote_path, pattern='/daily/')
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


def test_take_file_backup_with_aenc(master1,
                                    docker_client,
                                    gcs_client,
                                    config_content_files_aenc,
                                    gpg_public_key,
                                    gpg_private_key):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    backup_dir = "/etc/twindb"

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ['rm', '-f', gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['gpg',
           '--no-default-keyring',
           '--keyring', gpg_keyring,
           '--secret-keyring', gpg_secret_keyring,
           '--yes',
           '--no-tty',
           '--batch',
           '--import',
           gpg_private_key_path_guest]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    with open(twindb_config_host, 'w') as fp:
        content = config_content_files_aenc.format(
            TEST_DIR=backup_dir,
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring
        )
        fp.write(content)

    # write some content to the directory
    with open(os.path.join(twindb_config_dir, 'file'), 'w') as f:
        f.write("Hello world.")

    hostname = 'master1_1'
    gcs_backup_path = 'gs://%s/%s/hourly/files/%s' % \
                      (gcs_client.bucket, hostname, backup_dir.replace('/', '_'))

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'ls']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    assert gcs_backup_path in cout

    backup_to_restore = None
    for line in StringIO.StringIO(cout):
        if line.startswith(gcs_backup_path):
            backup_to_restore = line.strip()
            break
    assert backup_to_restore.endswith('.tar.gz.gpg')
    key = backup_to_restore.lstrip('gs://').lstrip(gcs_client.bucket).lstrip('/')
    local_copy = '%s/backup_to_restore.tar.gz.gpg' % twindb_config_dir
    gcs_client.gcs_client.download_file(
        gcs_client.bucket,
        key,
        local_copy
    )
    assert magic.from_file(local_copy) == 'data'

    dest_dir = '/tmp/simple_backup_aenc'
    cmd = ['mkdir', '-p', '/tmp/simple_backup_aenc']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'restore', 'file', '--dst', dest_dir, backup_to_restore]

    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    path_to_file_restored = '%s%s/file' % (dest_dir, backup_dir)
    cmd = ['test', '-f', path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    # And content is same
    path_to_file_orig = "%s/file" % backup_dir
    cmd = ['diff', '-Nur',
           path_to_file_orig,
           path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    assert not cout


def test__take_mysql_backup_aenc_suffix_gpg(master1,
                                            docker_client,
                                            gcs_client,
                                            config_content_mysql_aenc,
                                            gpg_public_key,
                                            gpg_private_key):
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

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ['rm', '-f', gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['gpg',
           '--no-default-keyring',
           '--keyring', gpg_keyring,
           '--secret-keyring', gpg_secret_keyring,
           '--yes',
           '--no-tty',
           '--batch',
           '--import',
           gpg_private_key_path_guest]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['twindb-backup',
           '--config', twindb_config_guest,
           'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    key = json.loads(cout)['daily'].keys()[0]
    assert key.endswith('xbstream.gz.gpg')
    local_copy = '%s/mysql_backup.tar.gz.gpg' % twindb_config_dir

    gcs_client.gcs_client.download_file(
        gcs_client.bucket,
        key,
        local_copy
    )
    assert magic.from_file(local_copy) == 'data'


def test_take_mysql_backup_aenc_restores_full(
    master1,
    docker_client,
    gcs_client,
    config_content_mysql_aenc,
    gpg_public_key,
    gpg_private_key,
    tmpdir
):
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

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ['rm', '-f', gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['gpg',
           '--no-default-keyring',
           '--keyring', gpg_keyring,
           '--secret-keyring', gpg_secret_keyring,
           '--yes',
           '--no-tty',
           '--batch',
           '--import',
           gpg_private_key_path_guest]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)
    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup',
           '--config', twindb_config_guest,
           'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    key = json.loads(cout)['daily'].keys()[0]

    backup_copy = 'gs://' + gcs_client.bucket + '/' + key
    dst_dir = str(tmpdir.mkdir('dst'))
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        ["mkdir", "-p", str(dst_dir)]
    )
    print(cout)
    assert ret == 0

    cmd = [
        'twindb-backup', '--debug',
        '--config', str(twindb_config_guest),
        'restore', 'mysql',
        backup_copy,
        '--dst', dst_dir
    ]

    # LOG.debug('Test paused')
    # LOG.debug(' '.join(cmd))
    # import time
    # time.sleep(36000)

    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        cmd
    )
    print(cout)
    assert ret == 0

    print('Files in restored datadir:')
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        ["find", dst_dir]
    )
    print(cout)
    assert ret == 0

    files_to_test = []
    for datadir_file in ['ibdata1', 'ib_logfile0', 'ib_logfile1',
                         'mysql/user.MYD',
                         'backup-my.cnf',
                         'xtrabackup_logfile']:
        files_to_test += [
            "test -f %s/%s" % (dst_dir, datadir_file)
        ]
    cmd = [
        "bash",
        "-c",
        " && ".join(files_to_test)
    ]

    print(cmd)
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        cmd
    )
    print(cout)
    assert ret == 0

    cmd = [
        "bash", "-c",
        "test -f {datadir}/_config/etc/my.cnf "
        "|| test -f {datadir}/_config/etc/mysql/my.cnf".format(
            datadir=dst_dir
        )
    ]
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        cmd
    )
    print(cout)
    assert ret == 0


def test_take_mysql_backup_aenc_restores_inc(
    master1,
    docker_client,
    gcs_client,
    config_content_mysql_aenc,
    gpg_public_key,
    gpg_private_key,
    tmpdir
):
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

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ['rm', '-f', gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['gpg',
           '--no-default-keyring',
           '--keyring', gpg_keyring,
           '--secret-keyring', gpg_secret_keyring,
           '--yes',
           '--no-tty',
           '--batch',
           '--import',
           gpg_private_key_path_guest]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    with open(twindb_config_host, 'w') as fp:
        content = config_content_mysql_aenc.format(
            GC_CREDENTIALS_FILE=os.environ['GC_CREDENTIALS_FILE'],
            BUCKET=gcs_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup',
           '--config', twindb_config_guest,
           'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    key = json.loads(cout)['hourly'].keys()[0]

    backup_copy = 'gs://' + gcs_client.bucket + '/' + key
    dst_dir = str(tmpdir.mkdir('dst'))
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        ["mkdir", "-p", str(dst_dir)]
    )
    print(cout)
    assert ret == 0

    cmd = [
        'twindb-backup', '--debug',
        '--config', str(twindb_config_guest),
        'restore', 'mysql',
        backup_copy,
        '--dst', dst_dir
    ]

    # LOG.debug('Test paused')
    # LOG.debug(' '.join(cmd))
    # import time
    # time.sleep(36000)

    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        cmd
    )
    print(cout)
    assert ret == 0

    print('Files in restored datadir:')
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        ["find", dst_dir]
    )
    print(cout)
    assert ret == 0

    files_to_test = []
    for datadir_file in ['ibdata1', 'ib_logfile0', 'ib_logfile1',
                         'mysql/user.MYD',
                         'backup-my.cnf',
                         'xtrabackup_logfile']:
        files_to_test += [
            "test -f %s/%s" % (dst_dir, datadir_file)
        ]
    cmd = [
        "bash",
        "-c",
        " && ".join(files_to_test)
    ]

    print(cmd)
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        cmd
    )
    print(cout)
    assert ret == 0

    cmd = [
        "bash", "-c",
        "test -f {datadir}/_config/etc/my.cnf "
        "|| test -f {datadir}/_config/etc/mysql/my.cnf".format(
            datadir=dst_dir
        )
    ]
    ret, cout = docker_execute(
        docker_client,
        master1['Id'],
        cmd
    )
    print(cout)
    assert ret == 0
