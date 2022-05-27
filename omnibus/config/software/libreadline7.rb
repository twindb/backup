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

name "libreadline7"
default_version "7.0"

version "7.0" do
  source sha256: "750d437185286f40a369e1e4f4764eda932b9459b5ec9a731628393dd3d32334"
  source url: "http://archive.ubuntu.com/ubuntu/pool/main/r/readline/readline_#{version}.orig.tar.gz"
end

license "GNU"
license_file "README"
skip_transitive_dependency_licensing true

relative_path "readline-#{version}"

build do
    env = with_standard_compiler_flags
    configure env: env

    make "-j #{workers}", env: env
    make "-j #{workers} install", env: env
end
