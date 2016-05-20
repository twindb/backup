#!/usr/bin/env bash

set -exu

packages="python-pip rpm-build ruby-devel gcc"

rpm -Uvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-6.noarch.rpm

yum -y install ${packages}
pip install --upgrade pip
pip install -U awscli

gem install fpm
