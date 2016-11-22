%{!?release: %global release 1}
%{!?PY_MAJOR: %global PY_MAJOR *}

Name:       twindb-backup
Version:    %{version}
Release:    %{release}
Summary:    Scripts to backup TwinDB infrastructure server

Group:      Applications/Databases
License:    Apache Software License 2.0
URL:        https://twindb.com
Source0:    %{name}-%{version}.tar.gz
BuildRoot:  %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildArch:  noarch

# Disable byte compilinig. Yikes!
# hard links fail on vagrant machines. Comment it out if that's fixed
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')

BuildRequires: coreutils
BuildRequires: redhat-rpm-config
BuildRequires: python >= 2.6.0

Requires:   python >= 2.6.0
Requires:   python-click
Requires:   MySQL-python
Requires:   python2-boto3
Requires:   python-psutil

Requires:   chkconfig

Requires:   initscripts
Requires:   cronie
Requires:   logrotate

%description
Scripts to backup TwinDB infrastructure server

%prep
%setup -q

%build
echo "Building twindb-backup"

%install
%__rm -rf %{buildroot}
%__make install DESTDIR=%{buildroot}

%post

%clean
%__rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%doc
%{_bindir}/twindb-backup
%config(noreplace) %attr(600, root, root) %{_sysconfdir}/twindb/twindb-backup.cfg
%config(noreplace) %attr(644, root, root) %{_sysconfdir}/cron.d/twindb-backup
%{python_sitelib}/twindb_backup
%{python_sitelib}/twindb_backup-%{version}-py%{PY_MAJOR}.egg-info


%changelog
* Sun Oct 30 2016 Aleksandr Kuzminsky <aleks@twindb.com>  2.0.0
  - Python implementation of twindb-backup


