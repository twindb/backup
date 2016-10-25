class profile::master {
    include profile::base

    file { '/etc/my.cnf':
        ensure => present,
        owner => 'mysql',
        source => 'puppet:///modules/profile/my-master.cnf',
        notify => Service['mysql'],
        require => Package['Percona-Server-server-56']
    }

    exec { 'Create table for checksumming':
        path    => '/usr/bin:/usr/sbin',
        user    => $profile::base::user,
        command => "mysql -e 'CREATE TABLE test.t1(id int not null primary key auto_increment, name varchar(255));
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
        require => [ Service['mysql'],
            File["/home/${profile::base::user}/.my.cnf"]],
        unless => 'mysql -e "DESC test.t1"'
    }

    exec { 'Run pt-tc':
        path    => '/usr/bin:/usr/sbin',
        user    => $profile::base::user,
        command => "pt-table-checksum --defaults-file /home/${profile::base::user}/.my.cnf",
        require => [ Service['mysql'], Exec['Create table for checksumming'],
            File["/home/${profile::base::user}/.my.cnf"]],
        unless => 'mysql -e "SHOW TABLES FROM percona"',
        returns => [16, 8]
    }

    file { "/etc/twindb":
        ensure => directory,
        owner  => 'root',
        mode   => "0700"
    }

    file { "/etc/twindb/twindb-backup.cfg":
        ensure => present,
        owner  => 'root',
        mode   => "0600",
        source => 'puppet:///modules/profile/twindb-backup.cfg',
        require => File['/etc/twindb']
    }
}
