#!/usr/bin/env bash

set -eux
yum install -y epel-release
yum install -y  http://www.percona.com/downloads/percona-release/redhat/0.1-3/percona-release-0.1-3.noarch.rpm


yum install -y \
  gcc \
  python-devel \
  zlib-devel \
  openssl-devel \
  make \
  python-setuptools \
  python-pip \
  Percona-Server-server-56 \
  Percona-Server-devel-56 \
  percona-xtrabackup

mysql_install_db && mysqld --user=root &

timeout=300
while [ ${timeout} -gt 0 ] ; do mysqladmin ping && break; sleep 1; timeout=$((${timeout} - 1)); done

cp -Rv /twindb-backup /tmp/
pip install /tmp/twindb-backup

make -C /tmp/twindb-backup test test-integration
