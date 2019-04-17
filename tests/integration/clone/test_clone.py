import time
from tests.integration.conftest import get_twindb_config_dir, docker_execute, \
    pause_test
from twindb_backup import INTERVALS, LOG
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


def test_clone(
        runner,
        master1,
        slave,
        docker_client,
        config_content_clone,
        client_my_cnf,
        rsa_private_key):

    twindb_config_dir = get_twindb_config_dir(docker_client, runner['Id'])
    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    private_key_host = "%s/private_key" % twindb_config_dir
    private_key_guest = "/etc/twindb/private_key"

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    with open(private_key_host, "w") as key_fd:
        key_fd.write(rsa_private_key)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_clone.format(
            PRIVATE_KEY=private_key_guest,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = '/usr/sbin/sshd'
    LOG.info('Run SSH daemon on master1_1')
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)

    cmd = [
        'twindb-backup',
        '--debug',
        '--config', twindb_config_guest,
        'clone',
        'mysql',
        '%s:3306' % master1['ip'],
        '%s:3306' % slave['ip']
    ]
    pause_test(' '.join(cmd))
    ret, cout = docker_execute(docker_client, runner['Id'], cmd)
    print(cout)

    assert ret == 0
    sql_master_2 = RemoteMySQLSource({
        "ssh_host": slave['ip'],
        "ssh_user": 'root',
        "ssh_key": private_key_guest,
        "mysql_connect_info": MySQLConnectInfo(
            my_cnf_path,
            hostname=slave['ip']
        ),
        "run_type": INTERVALS[0],
        "backup_type": 'full'
    })

    timeout = time.time() + 30
    while time.time() < timeout:
        with sql_master_2.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SHOW SLAVE STATUS')
                row = cursor.fetchone()
                if row['Slave_IO_Running'] == 'Yes' \
                        and row['Slave_SQL_Running'] == 'Yes':

                    LOG.info('Replication is up and running')
                    return

    LOG.error('Replication is not running after 30 seconds timeout')
    assert False
