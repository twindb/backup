import json
import os

from tests.integration.conftest import get_twindb_config_dir, docker_execute


def test__restore_mysql_inc_creates_log_files(master1,
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
    cmd = ['ls', '-la', '/var/lib/mysql']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--debug', '--config', twindb_config_guest, 'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup', '--config', twindb_config_guest, 'status']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    status = json.loads(cout)
    key = status['hourly'].keys()[0]
    backup_copy = 'gs://' + gcs_client.bucket + '/' + key
    dst_dir = '/tmp/dst_full_log_files'
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'restore', 'mysql',
           backup_copy,
           '--dst', dst_dir]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['find', dst_dir]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full_log_files/backup-my.cnf']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full_log_files/ibdata1']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full_log_files/ib_logfile0']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full_log_files/ib_logfile1']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full_log_files/mysql/user.MYD']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ['test', '-f', '/tmp/dst_full_log_files/xtrabackup_logfile']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
    cmd = ["bash", "-c", 'test -f /tmp/dst_full_log_files/_config/etc/my.cnf || '
                         'test -f /tmp/dst_full_log_files/_config/etc/mysql/my.cnf']
    print(cmd)
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0
