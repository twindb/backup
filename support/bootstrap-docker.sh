#!/usr/bin/env bash

set -eux
yum -y install epel-release
yum -y install http://www.percona.com/downloads/percona-release/redhat/0.1-3/percona-release-0.1-3.noarch.rpm


PACKAGES="
gcc
python-devel
zlib-devel
openssl-devel
rpm-build
make
python-setuptools
python-pip
/usr/bin/mysql_config
/usr/include/mysql/my_config.h
Percona-Server-server-56
Percona-Server-devel-56
percona-xtrabackup
"
for i in $(seq 5)
do
    yum -y install ${PACKAGES} && break
done

mysql_install_db && mysqld --user=root &

timeout=300
while [ ${timeout} -gt 0 ] ; do mysqladmin ping && break; sleep 1; timeout=$((${timeout} - 1)); done

cp -Rv /twindb-backup /tmp/
pip install /tmp/twindb-backup

make -C /tmp/twindb-backup test test-integration rpm

cp -R /tmp/twindb-backup/build /twindb-backup/
