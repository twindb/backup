#!/usr/bin/env bash

set -eux

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -qq \
  wget \
  lsb-release \
  zlib1g-dev \
  libssl-dev \
  make \
  libpython2.7-dev \
  python-setuptools \
  libffi-dev \
  git \
  gcc


easy_install pip

# install percona repository
wget https://repo.percona.com/apt/percona-release_0.1-4.$(lsb_release -sc)_all.deb
dpkg -i percona-release_0.1-4.$(lsb_release -sc)_all.deb
apt-get update

apt-get install -qq \
  percona-server-server-5.6 \
  libperconaserverclient18.1-dev \
  percona-xtrabackup-24

mysql_install_db && mysqld --user=root &

timeout=300
while [ ${timeout} -gt 0 ] ; do mysqladmin ping && break; sleep 1; timeout=$((${timeout} - 1)); done

cd /twindb-backup
pip install --editable .

make bootstrap lint test
