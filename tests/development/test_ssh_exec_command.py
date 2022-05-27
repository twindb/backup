import paramiko

from twindb_backup.destination.ssh import Ssh, SshConnectInfo


def test_ssh_exec_command():

    connect_info = SshConnectInfo(
        host="192.168.36.250",
        key="/Users/aleks/src/backup/vagrant/.vagrant/machines/master1/virtualbox/private_key",
        user="vagrant",
    )
    ssh = Ssh(ssh_connect_info=connect_info, remote_path="/tmp/aaa")
    _, stdout, stderr = ssh._execute_command(["/bin/ls", "/"])
    print(stdout.readlines())


# def test_get_remote_stdout():
#     connect_info = SshConnectInfo(
#         host='192.168.36.250',
#         key='/Users/aleks/src/backup/vagrant/.vagrant/machines/master1/virtualbox/private_key',
#         user='vagrant'
#     )
#     ssh = Ssh(ssh_connect_info=connect_info, remote_path='/tmp/aaa')
#
#     with ssh._get_remote_stdout(['ls', '/var']) as stdout:
#         print(stdout.readlines())

# def test_direct():
#     cli = paramiko.client.SSHClient()
#     cli.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
#     cli.connect(hostname="192.168.36.250", username="vagrant",
#                 key_filename='/Users/aleks/src/backup/vagrant/.vagrant/machines/master1/virtualbox/private_key')
#     stdin_, stdout_, stderr_ = cli.exec_command("ls -l /")
#     stdout_.channel.recv_exit_status()
#     lines = stdout_.readlines()
#     for line in lines:
#         print line
#
#
