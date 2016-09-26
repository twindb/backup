version = "`grep Version: twindb-backup.spec | awk '{ print $$2}' | head -1`"
release = "`grep Release: twindb-backup.spec | awk '{ print $$2}' | head -1`"
build_dir = .build
src_dir = twindb-backup-${version}
top_dir = /root/rpmbuild

.PHONY: rpmmacros

all:
	@echo "Ready"

install:
	mkdir -p "${DESTDIR}/etc/twindb"
	mkdir -p "${DESTDIR}/etc/cron.d"
	mkdir -p "${DESTDIR}/usr/bin"
	install -m 700 -o root twindb-backup.sh "${DESTDIR}/usr/bin"
	install -m 644 -o root twindb-backup "${DESTDIR}/etc/cron.d"
	test -f "${DESTDIR}/etc/twindb/twindb-backup.cfg" || install -m 600 -o root twindb-backup.cfg "${DESTDIR}/etc/twindb"

rpm: rpmmacros check-rpmbuild top_dir
	rm -rf "${build_dir}"
	mkdir -p "${build_dir}/${src_dir}"
	cp -R * "${build_dir}/${src_dir}"
	tar zcf "${top_dir}/SOURCES/${src_dir}.tar.gz" -C "${build_dir}" "${src_dir}"
	rpmbuild -ba twindb-backup.spec
	# rpm --addsign ${top_dir}/RPMS/noarch/twindb-backup-${version}-${release}.noarch.rpm

check-rpmbuild:
	 @which rpmbuild || yum -y install rpm-build

top_dir:
	mkdir -p "${top_dir}/SOURCES"

sign: rpm
	rpm --addsign ${top_dir}/RPMS/noarch/twindb-backup-*

rpmmacros:
	if ! test -f ~/.rpmmacros ; then cp rpmmacros ~/.rpmmacros; fi

# Build RPM inside a docker container
docker-rpm:
	sudo docker run -v `pwd`:/twindb-backup:rw  centos:centos${OS_VERSION} /bin/bash -c \
	"yum -y install rpm-build; pwd; ls -la"

deb: check-fpm
	rm -rf /tmp/installdir
	mkdir /tmp/installdir
	make install DESTDIR=/tmp/installdir
	rm -f twindb-backup-${version}-${release}_noarch.deb
	fpm -s dir -t deb -n twindb-backup -v ${version}-${release} \
		-C /tmp/installdir -p twindb-backup-${version}-${release}_noarch.deb \
		-d tar -d openssh-client -d cron \
		etc/cron.d etc/twindb usr/bin

check-fpm:
	@if test -z "`which fpm`"; then echo -e "Error: fpm is not found. Please install it:\n\nyum install ruby-devel gcc\ngem install fpm\n\nVisit https://github.com/jordansissel/fpm for more details"; exit -1; fi
clean:
	rm -rf .build
