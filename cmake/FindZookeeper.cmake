find_path(ZOOKEEPER_ROOT_DIR
    NAMES include/zookeeper/zookeeper.h
)

find_path(ZOOKEEPER_INCLUDE_DIR
    NAMES zookeeper/zookeeper.h
    HINTS ${ZOOOKEEPER_ROOT_DIR}/include
)

set (HINT_DIR ${ZOOKEEPER_ROOT_DIR}/lib)

find_library(ZOOKEEPER_LIBRARY
    NAMES zookeeper_mt zookeeper_st
    HINTS ${HINT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZOOKEEPER DEFAULT_MSG
    ZOOKEEPER_LIBRARY
    ZOOKEEPER_INCLUDE_DIR
)

include(CheckCXXSourceCompiles)

set(CMAKE_REQUIRED_LIBRARIES ${ZOOKEEPER_LIBRARY})
check_cxx_source_compiles("int main() { return 0; }" ZOOKEEPER_LINKS)
set(CMAKE_REQUIRED_LIBRARIES)

mark_as_advanced(
    ZOOKEEPER_ROOT_DIR
    ZOOKEEPER_INCLUDE_DIR
    ZOOKEEPER_LIBRARY
)
