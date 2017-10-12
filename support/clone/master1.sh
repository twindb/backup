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


rpm -q epel-release || install_package epel-release
rpm -q percona-release || install_package http://www.percona.com/downloads/percona-release/redhat/0.1-3/percona-release-0.1-3.noarch.rpm

install_package \
    Percona-Server-server-56 \
    Percona-Server-devel-56 \
    percona-xtrabackup-24




