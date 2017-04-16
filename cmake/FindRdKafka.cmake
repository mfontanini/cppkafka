find_path(RDKAFKA_ROOT_DIR
    NAMES include/librdkafka/rdkafka.h
)

find_path(RDKAFKA_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
    HINTS ${RDKAFKA_ROOT_DIR}/include
)

set (HINT_DIR ${RDKAFKA_ROOT_DIR}/lib)

find_library(RDKAFKA_LIBRARY
    NAMES rdkafka
    HINTS ${HINT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG
    RDKAFKA_LIBRARY
    RDKAFKA_INCLUDE_DIR
)

include(CheckFunctionExists)

set(CMAKE_REQUIRED_LIBRARIES ${RDKAFKA_LIBRARY})
check_function_exists(rd_kafka_committed HAVE_VALID_KAFKA_VERSION)
check_function_exists(rd_kafka_offsets_for_times HAVE_OFFSETS_FOR_TIMES)
set(CMAKE_REQUIRED_LIBRARIES)

if (HAVE_VALID_KAFKA_VERSION)
    message(STATUS "Found valid rdkafka version")
    mark_as_advanced(
        RDKAFKA_ROOT_DIR
        RDKAFKA_INCLUDE_DIR
        RDKAFKA_LIBRARY
    )
else()
    message(FATAL_ERROR "Failed to find valid rdkafka version")
endif()
