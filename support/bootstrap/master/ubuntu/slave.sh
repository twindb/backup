#!/usr/bin/env bash

set -exu
apt-get update

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print __version__")

package="/twindb-backup/omnibus/pkg/twindb-backup_${TB_VERSION}-1_amd64.deb"

dpkg -I ${package} | grep Depends: | sed -e 's/Depends://' -e 's/,//g' | xargs apt-get -y install
dpkg -i ${package}
