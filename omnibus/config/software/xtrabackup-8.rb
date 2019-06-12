name 'xtrabackup-8'
default_version '8.0.6'

skip_transitive_dependency_licensing true
dependency 'libffi'

version('8.0.6') { source md5: 'fa2f53a3af0a0e8047a1e3d9a3f1e12a'}

source url: "https://s3.amazonaws.com/twindb-release/percona-xtrabackup-#{version}.tar.gz"

relative_path "percona-xtrabackup-#{version}"
whitelist_file /.*/

# workers = 1

build do
    env = with_standard_compiler_flags(with_embedded_path)
    command 'cmake -DBUILD_CONFIG=xtrabackup_release ' \
        "-DCMAKE_INSTALL_PREFIX=#{install_dir} " \
        '-DWITH_SSL=system ' \
        '-DWITH_MAN_PAGES=OFF ' \
        '-DDOWNLOAD_BOOST=1 ' \
        "-DWITH_BOOST=#{install_dir}/libboost " \
        '-DFORCE_INSOURCE_BUILD=1 ' \
        "-DINSTALL_BINDIR=#{install_dir}/embedded/xtrabackup-8/bin", env: env

    make "-j #{workers}", env: env
    make 'install', env: env
    delete "#{install_dir}/libboost"
    delete "#{install_dir}/xtrabackup-test"
    strip "#{install_dir}/embedded/xtrabackup-8/bin"
end
