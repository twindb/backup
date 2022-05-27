#!/usr/bin/env bash

set -exu


wait_time=2
# Install repo.mysql.com GPG key
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29
for _ in $(seq 5)
do
    apt-get update && break
    echo "Waiting ${wait_time} seconds before retry"
    sleep ${wait_time}
    wait_time=$((wait_time * 2))
done

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print(__version__)")

package="/twindb-backup/omnibus/pkg/twindb-backup_${TB_VERSION}-1_amd64.deb"

dpkg -I "${package}" | grep Depends: | sed -e 's/Depends://' -e 's/,//g' | xargs apt-get -y install
dpkg -i "${package}"

set +u
if ! test -z "${DEV}"; then
    /bin/cp -R /twindb-backup/twindb_backup /opt/twindb-backup/embedded/lib/python3.9/site-packages
fi
