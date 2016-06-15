find_path(ZOOKEEPER_ROOT_DIR
    NAMES include/zookeeper/zookeeper.h
)

find_path(ZOOKEEPER_INCLUDE_DIR
    NAMES zookeeper/zookeeper.h
    HINTS ${ZOOKEEPER_ROOT_DIR}/include
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

mark_as_advanced(
    ZOOKEEPER_ROOT_DIR
    ZOOKEEPER_INCLUDE_DIR
    ZOOKEEPER_LIBRARY
)
