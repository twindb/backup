#!/usr/bin/env bash

set -exu

packages="python-pip rpm-build ruby-devel gcc"

rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

yum -y install ${packages}
pip install --upgrade pip
pip install -U awscli

gem install fpm
