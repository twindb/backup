#!/usr/bin/env bash

set -exu

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print __version__")

wait_time=2
for i in $(seq 5)
do
    yum clean all
    yum -y install /twindb-backup/omnibus/pkg/twindb-backup-${TB_VERSION}-1.x86_64.rpm && break
    echo "Waiting ${wait_time} seconds before retry"
    sleep ${wait_time}
    let wait_time=${wait_time}*2
done

#/bin/cp -R /twindb-backup/twindb_backup \
#   /opt/twindb-backup/embedded/lib/python2.7/site-packages/twindb_backup-${TB_VERSION}-py2.7.egg
