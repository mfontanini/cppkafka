#!/usr/bin/env python

import sys

if len(sys.argv) != 2:
    raise Exception('Usage: %s path/to/CMakeLists.txt' % sys.argv[0])

cmake_lists_file = sys.argv[1]
major = "0"
minor = "0"
patch = "0"
with open(cmake_lists_file) as f:
    for line in f:
        if 'CPPKAFKA_VERSION_MAJOR ' in line:
            major = line.split()[-1].replace(')', '')
        elif 'CPPKAFKA_VERSION_MINOR ' in line:
            minor = line.split()[-1].replace(')', '')
        elif 'CPPKAFKA_VERSION_REVISION ' in line:
            patch = line.split()[-1].replace(')', '')
            break

version = '.'.join(str(item) for item in (major, minor, patch))

print version
