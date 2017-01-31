#
# Copyright 2016 TwinDB LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require "./lib/ostools.rb"

name "twindb-backup"
maintainer "TwinDB Packager (TwinDB packager key) <packager@twindb.com>"
homepage "https://twindb.com"

# and /opt/twindb-backup on all other platforms
install_dir "/opt/twindb-backup"

build_version do
  source :git, from_dependency: 'twindb-backup'
  output_format :semver
end

build_iteration 1

description 'backup and recovery tool for MySQL
 TwinDB Backup tool for files, MySQL et al.'

 # ------------------------------------
# Generic package information
# ------------------------------------

# .deb specific flags
package :deb do
  vendor 'TwinDB Packager (TwinDB packager key) <packager@twindb.com>'
  license 'Apache License Version 2.0'
  section 'database'
  priority 'optional'
end

# .rpm specific flags
package :rpm do
  vendor 'TwinDB Packager (TwinDB packager key) <packager@twindb.com>'
  dist_tag ''
  license 'Apache Software License 2.0'
  category 'Applications/Databases'
  priority 'extra'
  if ENV.has_key?('RPM_SIGNING_PASSPHRASE') and not ENV['RPM_SIGNING_PASSPHRASE'].empty?
    signing_passphrase "#{ENV['RPM_SIGNING_PASSPHRASE']}"
  end
end

# ------------------------------------
# OS specific DSLs and dependencies
# ------------------------------------

# Creates required build directories
dependency "preparation"

# twindb-backup dependencies/components
runtime_dependency 'percona-xtrabackup'

dependency 'python'
dependency 'pip'

# Debian
if debian?
  dependency 'libyaml'
  runtime_dependency 'openssh-client'
end

# RHEL/CentOS
if redhat?
  runtime_dependency 'openssh-clients'
  runtime_dependency 'initscripts'
  runtime_dependency 'cronie'
  runtime_dependency 'logrotate'
end

# twindb-backup
dependency 'twindb-backup'

# Version manifest file
dependency "version-manifest"

exclude "**/.git"
exclude "**/bundler/git"
