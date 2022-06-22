#!/usr/bin/env bash

set -ex

#CACHE_REPO=${PLATFORM}-${OS_VERSION}

cd /twindb-backup/omnibus
gem install bundler:2.0.2
bundle install --binstubs
#bin/omnibus cache populate

#git config --global user.email "omnibus@twindb.com"
#git config --global user.name "Omnibus Builder"

#mkdir -p cache
#aws s3 cp s3://omnibus-cache-twindb-backup/${CACHE_REPO} cache/ || true

#if test -f cache/${CACHE_REPO}; then
#    git clone --mirror \
#        cache/${CACHE_REPO} \
#        /var/cache/omnibus/cache/git_cache/opt/twindb-backup
#fi

ruby bin/omnibus build twindb-backup

#if ! test -f cache/${CACHE_REPO}; then
#    git --git-dir=/var/cache/omnibus/cache/git_cache/opt/twindb-backup \
#        bundle create cache/${CACHE_REPO} --tags
#    aws s3 cp cache/${CACHE_REPO} s3://omnibus-cache-twindb-backup/${CACHE_REPO}
#fi
