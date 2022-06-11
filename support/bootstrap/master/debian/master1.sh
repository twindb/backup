#!/usr/bin/env bash

set -exu


wait_time=2
for _ in $(seq 5)
do
    apt-get -qq update && break
    echo "Waiting ${wait_time} seconds before retry"
    sleep ${wait_time}
    wait_time=$((wait_time * 2))
done

apt-get -qqq -y install apt-utils

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print(__version__)")

package="/twindb-backup/omnibus/pkg/twindb-backup_${TB_VERSION}-1_amd64.deb"

apt -y install "$package"

set +u
if ! test -z "${DEV}"; then
    /bin/cp -R /twindb-backup/twindb_backup /opt/twindb-backup/embedded/lib/python3.9/site-packages
fi
