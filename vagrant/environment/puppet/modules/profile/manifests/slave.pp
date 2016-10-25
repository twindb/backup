class profile::slave {
    include profile::base

    file { '/etc/my.cnf':
        ensure => present,
        owner => 'mysql',
        source => 'puppet:///modules/profile/my-slave.cnf',
        notify => Service['mysql'],
        require => Package['Percona-Server-server-56']
    }
}
