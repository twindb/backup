#!/bin/bash -e

cd /twindb-backup/omnibus
bundle install --binstubs
bin/omnibus build twindb-backup
