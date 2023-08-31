class profile::master {

  include profile::base


  file { '/etc/mysql/my.cnf':
    ensure  => present,
    owner   => 'mysql',
    source  => 'puppet:///modules/profile/my-master.cnf',
    notify  => Service['mysql'],
    require => Package['mysql-server']
  }

  file { "/home/${profile::base::user}/mysql_grants.sql":
    ensure => present,
    owner  => $profile::base::user,
    mode   => "0400",
    source => 'puppet:///modules/profile/mysql_grants.sql',
  }

  file { "/usr/local/bin/mysql_grants.sh":
    ensure => present,
    owner  => 'root',
    mode   => "0755",
    source => 'puppet:///modules/profile/mysql_grants.sh',
  }

  exec { 'Create MySQL users':
    path    => '/usr/bin:/usr/sbin',
    command => "/usr/local/bin/mysql_grants.sh",
    require => [
      Service['mysql'],
      File["/home/${profile::base::user}/mysql_grants.sql"],
      File["/usr/local/bin/mysql_grants.sh"],
      File["/home/${profile::base::user}/.my.cnf"]
    ],
    unless  => 'mysql -e "SHOW GRANTS FOR dba@localhost"'
  }
  exec { 'Create table for checksumming':
    path    => '/usr/bin:/usr/sbin',
    command => "mysql -e 'CREATE DATABASE test;
    CREATE TABLE test.t1(id int not null primary key auto_increment, name varchar(255));
    INSERT INTO test.t1(name) SELECT RAND()*100;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;
    INSERT INTO test.t1(name) SELECT name FROM test.t1;'",
    require => [
      Service['mysql'],
      File["/root/.my.cnf"]
    ],
    unless => 'mysql -e "DESC test.t1"'
}

  exec { 'Run pt-tc':
    path    => '/usr/bin:/usr/sbin',
    command => "pt-table-checksum --defaults-file /root/.my.cnf",
    require => [
      Service['mysql'],
      Exec['Create table for checksumming'],
      File["/root/.my.cnf"]
    ],
    unless  => 'mysql -e "SHOW TABLES FROM percona"',
    returns => [16, 8]
  }

  service { 'mysql':
    ensure  => running,
    enable  => true,
    require => Package['mysql-server'],
  }
}
