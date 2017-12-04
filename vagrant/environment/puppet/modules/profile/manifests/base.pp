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

  $packages = [ 'vim-enhanced', 'nmap-ncat',
    'Percona-Server-client-56', 'Percona-Server-server-56',
    'Percona-Server-devel-56', 'Percona-Server-shared-56', 'percona-toolkit',
    'percona-xtrabackup',
    'python2-pip',
    'gcc', 'python-devel', 'zlib-devel', 'openssl-devel',
    'rpm-build','docker', 'strace']

  package { $packages:
    ensure  => installed,
    require => [
      Yumrepo['Percona'],
      Package['epel-release']
    ]
  }

  yumrepo { 'Percona':
    baseurl  => 'http://repo.percona.com/centos/$releasever/os/$basearch/',
    enabled  => 1,
    gpgcheck => 0,
    descr    => 'Percona',
    retries  => 3
  }

  package { 'epel-release':
    ensure => installed
  }

  package { ['tox', 'awscli']:
    ensure   => installed,
    provider => pip,
    require  => Package['python2-pip']
  }

  service { 'docker':
    ensure => running,
    enable => true,
    require => Package['docker'],
  }

  exec { 'net.ipv4.ip_forward':
    path    => '/bin:/sbin',
    command => '/sbin/sysctl net.ipv4.ip_forward=1',
    unless  => 'sysctl net.ipv4.ip_forward | grep "net.ipv4.ip_forward = 1"'
  }

  file { "/etc/twindb":
    ensure => directory,
    owner  => 'root',
    mode   => "0700"
  }

  file { "/etc/twindb/twindb-backup.cfg":
    ensure  => present,
    owner   => 'root',
    mode    => "0600",
    source  => 'puppet:///modules/profile/twindb-backup.cfg',
    require => File['/etc/twindb']
  }

}
