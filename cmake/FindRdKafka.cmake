find_path(RDKAFKA_ROOT_DIR
    NAMES include/librdkafka/rdkafka.h
)

find_path(RDKAFKA_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
    HINTS ${RDKAFKA_ROOT_DIR}/include
)

set(HINT_DIR ${RDKAFKA_ROOT_DIR}/lib)

find_library(RDKAFKA_LIBRARY
    NAMES rdkafka librdkafka
    HINTS ${HINT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG
    RDKAFKA_LIBRARY
    RDKAFKA_INCLUDE_DIR
)

set(CONTENTS "#include <librdkafka/rdkafka.h>\n #if RD_KAFKA_VERSION >= 0x00090400\n int main() { }\n #endif")
set(FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_version_test.c)
file(WRITE ${FILE_NAME} ${CONTENTS})

try_compile(HAVE_VALID_KAFKA_VERSION ${CMAKE_CURRENT_BINARY_DIR}
            SOURCES ${FILE_NAME}
            CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${RDKAFKA_INCLUDE_DIR}")

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
