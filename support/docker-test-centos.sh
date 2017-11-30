#!/usr/bin/env bash

set -eux
yum clean all
yum install -y epel-release
yum install -y  http://www.percona.com/downloads/percona-release/redhat/0.1-3/percona-release-0.1-3.noarch.rpm

for i in $(seq 5); do
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
      percona-xtrabackup-24 \
      libffi-devel \
      git && break
    sleep 5
done

mysql_install_db && mysqld --user=root &

timeout=300
while [ ${timeout} -gt 0 ] ; do mysqladmin ping && break; sleep 1; timeout=$((${timeout} - 1)); done

cd /twindb-backup

make bootstrap lint test test-integration-backup-s3
