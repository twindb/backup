class role::slave {
  include profile::slave

    exec { 'Configure replication on slave':
        path    => '/usr/bin:/usr/sbin',
        command => "mysql -u root -e \"STOP SLAVE; CHANGE MASTER TO MASTER_HOST = '192.168.35.250', MASTER_USER = 'repl', MASTER_PASSWORD = 'slavepass', MASTER_AUTO_POSITION = 1; START SLAVE;\"",
        require => Service['mysql']
    }
}
