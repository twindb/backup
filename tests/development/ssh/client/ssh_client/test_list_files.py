from twindb_backup.ssh.client import SshClient


def test_list_files(tmpdir):

    ssh = SshClient(
        host='192.168.36.250',
        key='/vagrant/.vagrant/machines/master1/virtualbox/private_key',
        user='vagrant'
    )
    root_dir = tmpdir.mkdir('foo')
    print(str(root_dir))
    files = ssh.list_files(
        str(root_dir)
    )
    print(files)

    with open(str(root_dir.join('bar.txt')), 'w') as fp:
        fp.write('xxx')

    print(
        ssh.list_files(
            str(root_dir)
        )
    )

    print('blah')
    print(
        ssh.list_files('blah')
    )

    subdir = root_dir.mkdir('subdir')
    with open(str(subdir.join('sub_bar.txt')), 'w') as fp:
        fp.write('xxx')

    print('subdir')
    print(
        ssh.list_files(
            str(root_dir),
            recursive=True
        )
    )
