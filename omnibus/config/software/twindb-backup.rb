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

# These options are required for all software definitions
name "twindb-backup"

license "Apache-2.0"
license_file "LICENSE"
skip_transitive_dependency_licensing true

local_twindb_backup_repo = ENV['LOCAL_TWINDB_BACKUP_REPO']
if local_twindb_backup_repo.nil? || local_twindb_backup_repo.empty?
  source git: 'https://github.com/twindb/backup.git'
else
  source path: ENV['LOCAL_TWINDB_BACKUP_REPO']
end

twindb_backup_branch = ENV['TWINDB_BACKUP_BRANCH']
if twindb_backup_branch.nil? || twindb_backup_branch.empty?
  default_version 'master'
else
  default_version twindb_backup_branch
end

relative_path 'twindb-backup'

build do
  # Setup a default environment from Omnibus - you should use this Omnibus
  # helper everywhere. It will become the default in the future.
  env = with_standard_compiler_flags(with_embedded_path)

  command "#{install_dir}/embedded/bin/python setup.py install" \
          " --prefix=#{install_dir}/embedded" \
          " --install-scripts=#{install_dir}/bin", env: env

  # support files
  mkdir "#{install_dir}/support"
  copy 'support/twindb-backup.cfg', "#{install_dir}/support/"
  copy 'support/twindb-backup.cron', "#{install_dir}/support/"

  # Remove the .pyc and .pyo files from the package and list them in a file
  # so that the prerm script knows which compiled files to remove
  command "echo '# DO NOT REMOVE/MODIFY - used by package removal tasks' > #{install_dir}/embedded/.py_compiled_files.txt"
  command "find #{install_dir}/embedded '(' -name '*.pyc' -o -name '*.pyo' ')' -type f -delete -print >> #{install_dir}/embedded/.py_compiled_files.txt"
end
