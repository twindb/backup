#!/bin/bash -e

###########################
#
# WARNING: You need to rebuild the docker images if you do any changes to this file
#
############################

PACKAGER_NAME="TwinDB Packager (TwinDB packager key)"
PACKAGER_EMAIL="packager@twindb.com"
PROJECT_NAME=twindb-backup
LOG_LEVEL=${LOG_LEVEL:-"info"}
PROJECT_DIR=$(dirname $(realpath $0))

set -e

# We must run this script from the omnibus project directory
cd $PROJECT_DIR

# Clean up omnibus artifacts
rm -rf /var/cache/omnibus/pkg/*

# Clean up what we installed
rm -rf /etc/twindb
rm -rf /opt/$PROJECT_NAME/*

# If an RPM_SIGNING_PASSPHRASE has been passed, let's import the signing key
if [ -n "$RPM_SIGNING_PASSPHRASE" ]; then
  gpg --import /keys/RPM-SIGNING-KEY.private
fi

# Last but not least, let's make sure that we rebuild the agent everytime because
# the extra package files are destroyed when the build container stops (we have
# to tweak omnibus-git-cache directly for that).
git --git-dir=/var/cache/omnibus/cache/git_cache/opt/twindb-backup tag -d `git --git-dir=/var/cache/omnibus/cache/git_cache/opt/twindb-backup tag -l | grep twindb-backup` || true

# Setup git config
git config --global user.email "$PACKAGER_EMAIL"
git config --global user.name "$PACKAGER_NAME"

# Install the gems we need, with stubs in bin/
bundle update # Make sure to update to the latest version of omnibus-software
bin/omnibus build -l=$LOG_LEVEL $PROJECT_NAME
