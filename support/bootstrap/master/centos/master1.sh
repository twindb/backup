#!/usr/bin/env bash

set -exu

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print __version__")

wait_time=2
for _ in $(seq 5)
do
    yum clean all
    yum -y install /twindb-backup/omnibus/pkg/twindb-backup-"${TB_VERSION}"-1.x86_64.rpm && break
    echo "Waiting ${wait_time} seconds before retry"
    sleep ${wait_time}
    wait_time=$((wait_time * 2))
done

set +u
if ! test -z "${DEV}"; then
    /bin/cp -R /twindb-backup/twindb_backup /opt/twindb-backup/embedded/lib/python3.9/site-packages
fi

# MySQL sets random root password. Reset to empty
systemctl stop mysqld
rm -rf /var/lib/mysql
mkdir /var/lib/mysql
mysqld --initialize-insecure
chown -R mysql:mysql /var/lib/mysql
systemctl start mysqld
