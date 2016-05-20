#!/usr/bin/env bash

set -exu

packages="python-pip
mysql-server"

apt-get update
export DEBIAN_FRONTEND=noninteractive

apt-get -y install ${packages}
pip install -U awscli
