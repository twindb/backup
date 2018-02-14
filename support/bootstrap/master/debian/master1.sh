#!/usr/bin/env bash

set -exu
apt-get update

function install_package() {

    for i in 1 2 3
    do
        DEBIAN_FRONTEND=noninteractive apt-get -y install $@ && break
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
    mkdir /var/run/sshd
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
    /bin/cp -f /twindb-backup/vagrant/environment/puppet/modules/profile/files/my-master-legacy.cnf /etc/mysql/my.cnf

    /bin/chown -R mysql:mysql /var/lib/mysql
    /usr/sbin/mysqld --user=root
}

install_package curl
mysql_repo=mysql-apt-config_0.8.9-1_all.deb
curl --location https://dev.mysql.com/get/${mysql_repo} > /tmp/${mysql_repo}

install_package lsb-release wget
DEBIAN_FRONTEND=noninteractive dpkg -i /tmp/${mysql_repo}
apt-get update

#install_package "https://dev.mysql.com/get/mysql57-community-release-el${centos_version}-11.noarch.rpm"

install_package netcat sudo


install_package openssh-client openssh-server
start_sshd

MYSQL_PASSORD="MyNewPass"
debconf-set-selections <<< "mysql-community-server mysql-community-server/root-pass password ${MYSQL_PASSORD}"
debconf-set-selections <<< "mysql-community-server mysql-community-server/re-root-pass password ${MYSQL_PASSORD}"

install_package mysql-community-server mysql-community-client
start_mysqld
