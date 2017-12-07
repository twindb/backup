#!/usr/bin/env bash

if [ "$OS_VERSION" = 6 ] ; then
    sudo yum install -y centos-release-scl
    sudo yum install -y python27
    scl enable python27 bash
fi
