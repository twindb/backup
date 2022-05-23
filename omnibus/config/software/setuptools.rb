#
# Copyright 2013-2014 Chef Software, Inc.
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

name "setuptools"
default_version "62.3.2"

license "Python Software Foundation"
license_file "https://raw.githubusercontent.com/pypa/setuptools/master/LICENSE"
skip_transitive_dependency_licensing true

dependency "python"

version "51.3.3" do
  source md5: "83c2360e359139957c41c0798b2f4b67"
  source url: "https://pypi.org/packages/source/s/setuptools/setuptools-#{version}.tar.gz"
end

version "62.3.2" do
  source md5: "422e7b02215d6246aa061673ba0896f7"
  source url: "https://pypi.org/packages/source/s/setuptools/setuptools-#{version}.tar.gz"
end

relative_path "setuptools-#{version}"

build do
  env = with_standard_compiler_flags(with_embedded_path)

  command "#{install_dir}/embedded/bin/python setup.py install" \
          " --prefix=#{install_dir}/embedded", env: env
end
