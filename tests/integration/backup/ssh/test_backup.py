from tests.integration.conftest import get_twindb_config_dir, docker_execute


def test_backup(master1, storage_server,
                config_content_ssh,
                docker_client):

    twindb_config_dir = get_twindb_config_dir(docker_client, master1['Id'])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = '/etc/twindb/twindb-backup-1.cfg'
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    ssh_key_host = "%s/id_rsa" % twindb_config_dir
    ssh_key_guest = '/etc/twindb/id_rsa'

    contents = """
[client]
user=dba
password=qwerty
"""
    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(contents)

    ssh_key = """-----BEGIN RSA PRIVATE KEY-----
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

    with open(ssh_key_host, "w") as ssh_fd:
        ssh_fd.write(ssh_key)

    with open(twindb_config_host, 'w') as fp:
        content = config_content_ssh.format(
            PRIVATE_KEY=ssh_key_guest,
            HOST_IP=storage_server['ip'],
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
           '--debug',
           '--config', twindb_config_guest,
           'backup', 'hourly']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['test', '-d', '/tmp/backup']

    ret, cout = docker_execute(docker_client, storage_server['Id'], cmd)
    print(cout)
    assert ret == 0
    dir_path = "/var/backup/local/master1_1/hourly/mysql"
    cmd = ["bash", "-c", "ls %s | wc -l" % dir_path]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd, tty=True)
    print(cout)
    assert ret == 0
    assert '1' in cout

    cmd = ['twindb-backup',
           '--debug',
           '--config', twindb_config_guest,
           'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    cmd = ['twindb-backup',
           '--debug',
           '--config', twindb_config_guest,
           'backup', 'daily']
    ret, cout = docker_execute(docker_client, master1['Id'], cmd)
    print(cout)
    assert ret == 0

    dir_path = "/var/backup/local/master1_1/daily/mysql"
    cmd = ["bash", "-c", "ls %s | wc -l" % dir_path]
    ret, cout = docker_execute(docker_client, master1['Id'], cmd, tty=True)
    print(cout)
    assert ret == 0
    assert '1' in cout
