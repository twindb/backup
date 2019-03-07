import time
from tests.integration.conftest import get_twindb_config_dir, docker_execute
from twindb_backup import INTERVALS, LOG
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource


def test_clone(runner, master1, slave, docker_client, config_content_clone):

    twindb_config_dir = get_twindb_config_dir(docker_client, runner['Id'])
    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    private_key_host = "%s/private_key" % twindb_config_dir
    private_key_guest = "/etc/twindb/private_key"
    contents = """
[client]
user=dba
password=qwerty
"""
    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEoAIBAAKCAQEAyXxAjPShNGAedbaEtltFI6A7RlsyI+4evxTq6uQrgbJ6Hm+p
HBXshXQYXDyVjvytaM+6GKF+r+6+C+6Wc5Xz4lLO/ZiSCdPbyEgqw1JoHrgPNpc6
wmCtjJExxjzvpwSVgbZg3xOdqW1y+TyqeUkXEg/Lm4VZhN1Q/KyGCgBlWuAXoOYR
GhaNWqcnr/Wn5YzVHAx2yJNrurtKLVYVMIkGcN/6OUaPpWqKZLaXiK/28PSZ5GdT
DmxRg4W0pdyGEYQndpPlpLF4w5gNUEhVZM8hWVE29+DIW3XXVYGYchxmkhU7wrGx
xZR+k5AT+7g8VspVS8zNMXM9Z27w55EQuluNMQIBIwKCAQAzz35QIaXLo7APo/Y9
hS8JKTPQQ1YJPTsbMUO4vlRUjPrUoF6vc1oTsCOFbqoddCyXS1u9MNdvEYFThn51
flSn6WhtGJqU0BPxrChA2q0PNqTThfkqqyVQCBQdCFrhzfqPEaPhl1RtZUlzSh01
IWxVGgEn/bfu9xTTQk5aV9+MZQ2XKe4BGzpOZMI/B7ivRCcthEwMTx92opr52bre
4t7DahVLN/2Wu4lxajDzCaKXpjMuL76lFov0mZZN7S8whH5xSx1tpapHqsCAwfLL
k49lDdR8aN6oqoeK0e9w//McIaKxN2FVxD4bcuXiQTjihx+QwQOLmlHSRDKhTsYg
4Q5bAoGBAOgVZM2eqC8hNl5UH//uuxOeBKqwz7L/FtGemNr9m0XG8N9yE/K7A5iX
6EDvDyVI51IlIXdxfK8re5yxfbJ4YevenwdEZZ2O8YRrVByJ53PV9CcVeWL4p6f/
I56sYyDfXcnDTEOVYY0mCfYUfUcSb1ExpuIU4RvuQJg6tvbdxD9FAoGBAN4/pVCT
krRd6PJmt6Dbc2IF6N09OrAnLB3fivGztF5cp+RpyqZK4ve+akLoe1laTg7vNtnF
l/PZtM9v/VT45hb70MFEHO+sKvGa5Yimxkb6YCriJOcLxTysSgFHKz7v+8BqqoHi
qY4fORGwPVDv28I8jKRvcuNHendV/Rdcuk79AoGAd1t1q5NscAJzu3u4r4IXEWc1
mZzClpHROJq1AujTgviZInUu1JqxZGthgHrx2KkmggR3nIOB86/2bdefut7TRhq4
L5+Et24VzxKgSTD6sJnrR0zfV3iQvMxbdizFRBsaSoGyMWLEdHn2fo4xzMem9o6Q
VwNsdMOsMQhA1rsxuiMCgYBr8wcnIxte68jqxC1OIXKOsmnKi3RG7nSDidXF2vE1
JbCiJMGD+Hzeu5KyyLDw4rgzI7uOWKjkJ+obnMuBCy3t6AZPPlcylXPxsaKwFn2Q
MHfaUJWUyzPqRQ4AnukekdINAJv18cAR1Kaw0fHle9Ej1ERP3lxfw6HiMRSHsLJD
nwKBgCIXVhXCDaXOOn8M4ky6k27bnGJrTkrRjHaq4qWiQhzizOBTb+7MjCrJIV28
8knW8+YtEOfl5R053SKQgVsmRjjDfvCirGgqC4kSAN4A6MD+GNVXZVUUjAUBVUbU
8Wt4BxW6kFA7+Su7n8o4DxCqhZYmK9ZUhNjE+uUhxJCJaGr4
-----END RSA PRIVATE KEY-----
"""
    with open(private_key_host, "w") as key_fd:
        key_fd.write(private_key)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_clone.format(
            PRIVATE_KEY=private_key_guest,
            MY_CNF='/etc/twindb/my.cnf'
        )
        fp.write(content)

    cmd = '/usr/sbin/sshd'
    # Run SSH daemon on master1_1
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)

    cmd = ['twindb-backup', '--debug',
           '--config', twindb_config_guest,
           'clone', 'mysql',
           "%s:3306" % master1['ip'], "%s:3306" % slave['ip']
           ]
    # LOG.debug('Test paused')
    # LOG.debug(' '.join(cmd))
    # import time
    # time.sleep(36000)

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
                if row['Slave_IO_Running'] == 'Yes' and row['Slave_SQL_Running'] == 'Yes':
                    LOG.info('Relication is up and running')
                    return
    LOG.error('Replication is not running after 30 seconds timeout')
    assert False
