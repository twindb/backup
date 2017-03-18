#!/bin/bash -e

cd /twindb-backup/omnibus
bundle install --binstubs
omnibus build twindb-backup
