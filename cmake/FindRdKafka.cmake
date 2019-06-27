# This find module helps find the RdKafka module. It exports the following variables:
# - RdKafka_INCLUDE_DIR : The directory where rdkafka.h is located.
# - RdKafka_LIBNAME : The name of the library, i.e. librdkafka.a, librdkafka.so, etc.
# - RdKafka_LIBRARY_DIR : The directory where the library is located.
# - RdKafka_LIBRARY_PATH : The full library path i.e. ${RdKafka_LIBRARY_DIR}/${RdKafka_LIBNAME}
# - RdKafka_DEPENDENCIES : Libs needed to link with RdKafka

if (CPPKAFKA_RDKAFKA_STATIC_LIB)
    set(RDKAFKA_PREFIX ${CMAKE_STATIC_LIBRARY_PREFIX})
    set(RDKAFKA_SUFFIX ${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(RDKAFKA_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(RDKAFKA_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()

set(RdKafka_LIBNAME ${RDKAFKA_PREFIX}rdkafka${RDKAFKA_SUFFIX})

find_path(RdKafka_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
    HINTS ${RdKafka_ROOT}/include
)

find_path(RdKafka_LIBRARY_DIR
    NAMES ${RdKafka_LIBNAME} rdkafka
    HINTS ${RdKafka_ROOT}/lib ${RdKafka_ROOT}/lib64
)

find_library(RdKafka_LIBRARY_PATH
    NAMES ${RdKafka_LIBNAME} rdkafka
    HINTS ${RdKafka_LIBRARY_DIR}
)

# Check lib paths
if (CPPKAFKA_CMAKE_VERBOSE)
    get_property(FIND_LIBRARY_32 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB32_PATHS)
    get_property(FIND_LIBRARY_64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
    message(STATUS "RDKAFKA search 32-bit library paths: ${FIND_LIBRARY_32}")
    message(STATUS "RDKAFKA search 64-bit library paths: ${FIND_LIBRARY_64}")
    message(STATUS "RdKafka_ROOT = ${RdKafka_ROOT}")
    message(STATUS "RdKafka_INCLUDE_DIR = ${RdKafka_INCLUDE_DIR}")
    message(STATUS "RdKafka_LIBNAME = ${RdKafka_LIBNAME}")
    message(STATUS "RdKafka_LIBRARY_PATH = ${RdKafka_LIBRARY_PATH}")
    message(STATUS "RdKafka_LIBRARY_DIR = ${RdKafka_LIBRARY_DIR}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG
    RdKafka_LIBNAME
    RdKafka_LIBRARY_DIR
    RdKafka_LIBRARY_PATH
    RdKafka_INCLUDE_DIR
)

set(CONTENTS "#include <librdkafka/rdkafka.h>\n #if RD_KAFKA_VERSION >= ${RDKAFKA_MIN_VERSION_HEX}\n int main() { }\n #endif")
set(FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_version_test.c)
file(WRITE ${FILE_NAME} ${CONTENTS})

try_compile(RdKafka_FOUND ${CMAKE_CURRENT_BINARY_DIR}
            SOURCES ${FILE_NAME}
            CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${RdKafka_INCLUDE_DIR}")

if (RdKafka_FOUND)
    if (CPPKAFKA_RDKAFKA_STATIC_LIB)
        set(RdKafka_DEPENDENCIES ${RdKafka_LIBNAME} pthread rt ssl crypto dl z)
    else()
        set(RdKafka_DEPENDENCIES ${RdKafka_LIBNAME} pthread)
    endif()
    include_directories(SYSTEM ${RdKafka_INCLUDE_DIR})
    link_directories(${RdKafka_LIBRARY_DIR})
    message(STATUS "Found valid rdkafka version")
    mark_as_advanced(
        RDKAFKA_LIBRARY
        RdKafka_LIBRARY_DIR
        RdKafka_INCLUDE_DIR
    )
else()
    message(FATAL_ERROR "Failed to find valid rdkafka version")
endif()
