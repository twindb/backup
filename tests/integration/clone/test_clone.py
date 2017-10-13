import time
from click.testing import CliRunner

from twindb_backup import INTERVALS
from twindb_backup.cli import main
from twindb_backup.destination.ssh import SshConnectInfo, Ssh
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.util import split_host_port


def test_clone(master1, master2, config_content_clone, tmpdir):

    my_cnf = tmpdir.join('.my.cnf')
    my_cnf.write("""
[client]
user=dba
password=qwerty
""")
    config = tmpdir.join('twindb-backup.cfg')
    content = config_content_clone.format(
        PRIVATE_KEY="/twindb_backup/vagrant/environment/puppet/modules/profile/files/id_rsa",
        MY_CNF=str(my_cnf)
    )

    config.write(content)
    runner = CliRunner()
    result = runner.invoke(main,
                           ['--config', str(config),
                            '--debug',
                            'clone', 'mysql', master1['ip'], master2['ip']]
                           )
    if result.exit_code != 0:
        print('Command output:')
        print(result.output)
        print(result.exception)
        print(result.exc_info)
    assert result.exit_code == 0

    sql_master_2 = RemoteMySQLSource({
        "ssh_connection_info": SshConnectInfo(
            host=master2['ip'],
            user='root',
            key="/twindb_backup/vagrant/environment/puppet/modules/profile/files/id_rsa"
        ),
        "mysql_connect_info": MySQLConnectInfo(
            "/root/.my.cnf",
            hostname=master2['ip']
        ),
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
    })

    ssh_master_2 = Ssh(
        ssh_connect_info=SshConnectInfo(
            host=split_host_port(master2['ip'])[0],
            user='root',
            key="/twindb_backup/vagrant/environment/puppet/modules/profile/files/id_rsa"
        ),
    )
    assert ssh_master_2.list_files(sql_master_2.datadir)
