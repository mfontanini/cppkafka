Name:    cppkafka
Version: %{__version}
Release: %{__release}%{?dist}

Summary: Modern C++ Apache Kafka client library (wrapper for librdkafka)
Group:   Development/Libraries/C and C++
License: BSD-2-Clause
URL:     https://github.com/mfontanini/cppkafka
Source:  %{name}-%{version}.tar.gz

BuildRequires: librdkafka-devel cmake g++ boost-devel openssl-devel zlib-devel

%description
Modern C++ Apache Kafka client library (wrapper for librdkafka)

%package devel
Summary:        Development files for %{name}
Group:          Development/Libraries/C and C++
Requires:       %{name} = %{version}

%description devel
This package contains headers and libraries required to build applications
using cppkafka.

%global debug_package %%{nil}

%prep
%setup

%build
cd $RPM_BUILD_DIR
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=%{_prefix} \
      $RPM_BUILD_DIR/%{name}-%{version}
make -j$(nproc)

%install
cd $RPM_BUILD_DIR/build
DESTDIR=%{buildroot} make install

%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%pre
%preun

%files
%{_libdir}/lib%{name}*.so.*

%files devel
%{_libdir}/lib%{name}*.so*
%{_libdir}/../lib/cmake/CppKafka/*
%{_includedir}/%{name}
%{_datadir}/pkgconfig

%changelog
* Fri Feb 21 2020 Adam Fowles <adam.fowles@eagleview.com> 0.3.1-0
- Initial RPM package
