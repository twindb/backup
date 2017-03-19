#!/bin/bash -e

cd /twindb-backup/omnibus
bundle update
bundle install --binstubs
bin/omnibus build twindb-backup
