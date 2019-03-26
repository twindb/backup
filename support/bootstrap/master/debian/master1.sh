#!/usr/bin/env bash

set -exu

wait_time=2
for i in $(seq 5)
do
    apt-get update && break
    echo "Waiting ${wait_time} seconds before retry"
    sleep ${wait_time}
    let wait_time=${wait_time}*2
done


TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print __version__")

package="/twindb-backup/omnibus/pkg/twindb-backup_${TB_VERSION}-1_amd64.deb"

dpkg -I ${package} | grep Depends: | sed -e 's/Depends://' -e 's/,//g' | xargs apt-get -y install
dpkg -i ${package}

#/bin/cp -R /twindb-backup/twindb_backup \
#   /opt/twindb-backup/embedded/lib/python2.7/site-packages/twindb_backup-${TB_VERSION}-py2.7.egg
