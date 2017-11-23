#!/usr/bin/env bash

set -exu
yum clean all

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

start_sshd
