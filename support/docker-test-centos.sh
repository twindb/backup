#!/usr/bin/env bash

set -eux
yum clean all
yum install -y epel-release
yum install -y  http://www.percona.com/downloads/percona-release/redhat/0.1-4/percona-release-0.1-4.noarch.rpm

for i in $(seq 5); do
    yum install -y \
      gcc \
      zlib-devel \
      openssl-devel \
      make \
      wget \
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

if [ "$OS_VERSION" = 6 ] ; then
    rpm -ivh http://dl.iuscommunity.org/pub/ius/stable/Redhat/6/x86_64/epel-release-6-5.noarch.rpm
    rpm -ivh http://dl.iuscommunity.org/pub/ius/stable/Redhat/6/x86_64/ius-release-1.0-14.ius.el6.noarch.rpm
    yum install -y python27 python27-devel python27-setuptools python27-pip
    ln -sf /usr/bin/python2.7 /usr/bin/python
    /usr/bin/easy_install-2.7 pip
    /usr/bin/easy_install-2.7 setuptools
    make bootstrap lint test test-integration-backup-s3
else
    yum install -y python-devel python-setuptools python-pip
    make bootstrap lint test test-integration-backup-s3
fi
