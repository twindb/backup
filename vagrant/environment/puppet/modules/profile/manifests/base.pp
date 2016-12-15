class profile::base {
  $user = 'vagrant'
  user { $user:
    ensure => present
  }

  file { "/home/${user}":
    ensure => directory,
    owner  => $user,
    mode   => "0750"
  }

  file { "/home/${profile::base::user}/.bashrc":
    ensure => present,
    owner  => $profile::base::user,
    mode   => "0644",
    source => 'puppet:///modules/profile/bashrc',
  }

  file { '/root/.ssh':
    ensure => directory,
    owner => 'root',
    mode => '700'
  }

  file { '/root/.ssh/authorized_keys':
    ensure => present,
    owner => 'root',
    mode => '600',
    source => 'puppet:///modules/profile/id_rsa.pub'
  }

  file { '/root/.ssh/id_rsa':
    ensure => present,
    owner => 'root',
    mode => '600',
    source => 'puppet:///modules/profile/id_rsa'
  }

  file { "/home/${profile::base::user}/.my.cnf":
    ensure => present,
    owner  => $profile::base::user,
    mode   => "0600",
    content => "[client]
user=dba
password=qwerty
"
  }

  file { "/root/.my.cnf":
    ensure => present,
    owner  => 'root',
    mode   => "0600",
    content => "[client]
user=dba
password=qwerty
"
  }

  yumrepo { 'Percona':
    baseurl => 'http://repo.percona.com/centos/$releasever/os/$basearch/',
    enabled => 1,
    gpgcheck => 0,
    descr => 'Percona',
    retries => 3
  }

    package { 'epel-release':
        ensure => installed
    }

  $packages = [ 'vim-enhanced', 'nmap-ncat',
    'Percona-Server-client-56', 'Percona-Server-server-56',
    'Percona-Server-devel-56', 'Percona-Server-shared-56', 'percona-toolkit',
    'percona-xtrabackup',
    'python2-pip',
    'gcc', 'python-devel', 'zlib-devel', 'openssl-devel',
    'rpm-build','docker']

  package { $packages:
    ensure => installed,
    require => [Yumrepo['Percona'], Package['epel-release']]
  }

    package { ['tox', 'awscli']:
        ensure => installed,
        provider => pip,
        require => Package['python2-pip']
    }

  service { 'mysql':
    ensure => running,
    enable => true,
    require => Package['Percona-Server-server-56']
  }

  file { "/home/${profile::base::user}/mysql_grants.sql":
    ensure => present,
    owner  => $profile::base::user,
    mode   => "0400",
    source => 'puppet:///modules/profile/mysql_grants.sql',
  }

  exec { 'Create MySQL users':
    path    => '/usr/bin:/usr/sbin',
    user    => $profile::base::user,
    command => "mysql -u root < /home/${$profile::base::user}/mysql_grants.sql",
    require => [ Service['mysql'], File["/home/${profile::base::user}/mysql_grants.sql"] ],
    before => File["/home/${profile::base::user}/.my.cnf"],
    unless => 'mysql -e "SHOW GRANTS FOR dba@localhost"'
  }

    service { 'docker':
        ensure => running,
        enable => true,
        require => Package['docker']
    }

}
