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

try_run(_rdkafka_version_check_run_result _rdkafka_version_check_compile_result
        ${CMAKE_CURRENT_BINARY_DIR}/cmake
        ${CMAKE_MODULE_PATH}/sources/check_rdkafka_version.cpp
        CMAKE_FLAGS -DINCLUDE_DIRECTORIES:STRING=${RDKAFKA_INCLUDE_DIR} 
                    -DLINK_LIBRARIES:STRING=${RDKAFKA_LIBRARY})

if (${_rdkafka_version_check_compile_result} STREQUAL "TRUE")
    if (${_rdkafka_version_check_run_result} EQUAL 1)
        message(STATUS "Found valid rdkafka version")
        mark_as_advanced(
            RDKAFKA_ROOT_DIR
            RDKAFKA_INCLUDE_DIR
            RDKAFKA_LIBRARY
        )
    else()
        message(FATAL_ERROR "Invalid rdkafka version found (< 0.9)")
    endif()
else()
    message(FATAL_ERROR "Failed to find rdkafka")
endif()

