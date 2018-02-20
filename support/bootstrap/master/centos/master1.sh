#!/usr/bin/env bash

set -exu
yum clean all

function install_package() {

    for i in 1 2 3
    do
        yum -y install $@ && break
        sleep 5
    done
}

function start_sshd() {
    if ! test -f /etc/ssh/ssh_host_rsa_key; then
        /usr/bin/ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -P ""
    fi
    if ! test -f /etc/ssh/ssh_host_dsa_key; then
        /usr/bin/ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key -P ""
    fi
    # Run sshd in background
    /usr/sbin/sshd

    mkdir -p /root/.ssh/
    /bin/chown root:root /root/.ssh
    /bin/chmod 700 /root/.ssh/

    /bin/cp -f /twindb-backup/vagrant/environment/puppet/modules/profile/files/id_rsa.pub /root/.ssh/authorized_keys
    /bin/chown root:root /root/.ssh/authorized_keys
    /bin/chmod 600 /root/.ssh/authorized_keys

}


function start_mysqld() {
    /bin/cp -f /twindb-backup/vagrant/environment/puppet/modules/profile/files/my-master-legacy.cnf /etc/my.cnf
    init_file=/twindb-backup/vagrant/environment/puppet/modules/profile/files/mysql-init

    /usr/sbin/mysqld --initialize
    /bin/chown -R mysql:mysql /var/lib/mysql

    /usr/sbin/mysqld --init-file=${init_file}
}

rpm -q epel-release || install_package epel-release
centos_version=$(rpm --eval '%{centos}')


install_package "https://dev.mysql.com/get/mysql57-community-release-el${centos_version}-11.noarch.rpm"

install_package mysql-community-server mysql-community-client
install_package openssh-server nc sudo

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print __version__")

install_package /twindb-backup/omnibus/pkg/twindb-backup-${TB_VERSION}-1.x86_64.rpm

twindb-backup --version

start_sshd
start_mysqld
