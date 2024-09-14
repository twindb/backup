#!/usr/bin/env bash

set -exu
apt-get update

function install_package() {

    for _ in 1 2 3
    do
        # shellcheck disable=SC2068
        apt-get -y install $@ && break
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

    mkdir -p /run/sshd
    mkdir -p /root/.ssh/
    /bin/chown root:root /root/.ssh
    /bin/chmod 700 /root/.ssh/

    /bin/cp -f /twindb-backup/vagrant/environment/puppet/modules/profile/files/id_rsa.pub /root/.ssh/authorized_keys
    /bin/chown root:root /root/.ssh/authorized_keys
    /bin/chmod 600 /root/.ssh/authorized_keys

    /usr/sbin/sshd -D
}


install_package \
    openssh-server \
    netcat \
    sudo

start_sshd
