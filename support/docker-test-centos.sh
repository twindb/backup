#!/usr/bin/env bash

set -eux
yum clean all
yum install -y epel-release
yum install -y  http://www.percona.com/downloads/percona-release/redhat/0.1-4/percona-release-0.1-4.noarch.rpm

for i in $(seq 5); do
    yum install -y \
      gcc \
      zlib-devel \
      bzip2-devel \
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
    cd /tmp
    wget http://python.org/ftp/python/2.7.14/Python-2.7.14.tgz
    tar zxvf Python-2.7.14.tgz
    cd Python-2.7.14
    ./configure --prefix=/usr/local --enable-unicode=ucs4 --enable-shared LDFLAGS="-Wl,-rpath /usr/local/lib"
    make && make altinstall
    strip /usr/local/lib/libpython2.7.so.1.0
    wget https://bootstrap.pypa.io/get-pip.py
    python2.7 get-pip.py
    pip2.7 install virtualenv
    alias python=/usr/local/bin/python2.7
else
    yum install -y python-devel python-setuptools python-pip
fi

cd /twindb-backup
make bootstrap lint test test-integration-backup-s3
