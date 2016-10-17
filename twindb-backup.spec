Name:		twindb-backup
Version:    1.2.0
Release:	1
Summary:	Scripts to backup TwinDB infrastructure server

Group:		Applications/Databases
Vendor:     TwinDB LLC
License:    Apache Software License 2.0
URL:		https://twindb.com
Source0:	%{name}-%{version}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildArch:  noarch

Requires:	tar openssh-clients cronie

%description
Scripts to backup TwinDB infrastructure server

%prep
%setup -q


%build
make %{?_smp_mflags}


%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}


%clean
rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
%config(noreplace) %attr(600, root, root) %{_sysconfdir}/twindb/twindb-backup.cfg
%attr(644, root, root) %{_sysconfdir}/cron.d/twindb-backup
%attr(700, root, root) %{_bindir}/twindb-backup.sh


%changelog
* Sat Oct 04 2014 Aleksandr Kuzminsky <aleks@twindb.com> - 1.0.0
- deployed in production

* Sat Oct 04 2014 Aleksandr Kuzminsky <aleks@twindb.com> - 0.0
- Initial package.
