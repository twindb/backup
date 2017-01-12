#
# Copyright:: Copyright (c) 2013 Robby Dyer
# Copyright:: Copyright (c) 2014 GitLab.com
# Copyright:: 2016 TwinDB LLC
# License:: Apache License, Version 2.0
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
name "mysql-client"
default_version "5.6.33"

skip_transitive_dependency_licensing true

dependency "openssl"
dependency "zlib"
dependency "ncurses"
dependency "cmake"

source  :url => "http://dev.mysql.com/get/Downloads/MySQL-5.6/mysql-#{default_version}.tar.gz",
        :md5 => "7fbf37928ef651e005b80e820a055385"

relative_path "mysql-#{version}"

env = with_standard_compiler_flags(with_embedded_path)
env.merge!(
  "CXXFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
  "CPPFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
)

build do
  command ["which cmake"], :env => env
  command ["file #{install_dir}/embedded/lib/libssl.so"], :env => env
  command ["file #{install_dir}/embedded/lib/libz.so"], :env => env
  command ["file #{install_dir}/embedded/lib/libcrypto.so"], :env => env

  command [
            "cmake",
            "-DCMAKE_SKIP_RPATH=YES",
            "-DCMAKE_INSTALL_PREFIX=#{install_dir}/embedded",
            "-DWITH_SSL=system",
            "-DOPENSSL_INCLUDE_DIR:PATH=#{install_dir}/embedded/include",
            "-DOPENSSL_LIBRARIES:FILEPATH=#{install_dir}/embedded/lib/libssl.so",
            "-DWITH_ZLIB=system",
            "-DZLIB_INCLUDE_DIR:PATH=#{install_dir}/embedded/include",
            "-DZLIB_LIBRARY:FILEPATH=#{install_dir}/embedded/lib/libz.so",
            "-DCRYPTO_LIBRARY:FILEPATH=#{install_dir}/embedded/lib/libcrypto.so",
            ".",
           ].join(" "), :env => env

  %w{libmysql client include scripts}.each do |target|
    command "make -j #{workers} install", :env => env, :cwd => "#{project_dir}/#{target}"
  end
end
