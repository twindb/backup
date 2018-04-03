from twindb_backup.status.status import Status


def test_init_creates_instance(status_raw_content):
    status = Status(status_raw_content)
    assert status.version == 0
    assert isinstance(status.hourly, dict)
    assert isinstance(status.daily, dict)
    assert isinstance(status.weekly, dict)
    assert isinstance(status.monthly, dict)
    assert isinstance(status.yearly, dict)

    assert status.valid


def test_init_creates_empty():
    status = Status()
    assert status.version == 0
    assert isinstance(status.hourly, dict)
    assert isinstance(status.daily, dict)
    assert isinstance(status.weekly, dict)
    assert isinstance(status.monthly, dict)
    assert isinstance(status.yearly, dict)

    assert status.valid


def test_init_decodes_mycnf(status_raw_content):
    status = Status(status_raw_content)
    hourly_copy = 'master1/hourly/mysql/mysql-2018-03-28_04_11_16.xbstream.gz'
    assert status.hourly[hourly_copy]['config'][0]['/etc/my.cnf'] == """[mysqld]
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
user=mysql
# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0

server_id=100
gtid_mode=ON
log-bin=mysql-bin
log-slave-updates
enforce-gtid-consistency

[mysqld_safe]
log-error=/var/log/mysqld.log
pid-file=/var/run/mysqld/mysqld.pid
"""
