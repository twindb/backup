#!/usr/bin/env bash

set -exu

TB_VERSION=$(PYTHONPATH=/twindb-backup python -c "from twindb_backup import __version__; print __version__")

yum -y install /twindb-backup/omnibus/pkg/twindb-backup-${TB_VERSION}-1.x86_64.rpm
