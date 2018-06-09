# Local function to auto-generate main cppkafka.h header file
function(make_cppkafka_header)
    set(CPPKAFKA_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/cppkafka.h)
    file(GLOB INCLUDE_HEADERS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.h" "utils/*.h")
    list(SORT INCLUDE_HEADERS)
    foreach(header ${INCLUDE_HEADERS})
        if (NOT ${header} MATCHES "cppkafka.h")
            SET(CPPKAFKA_HEADERS "${CPPKAFKA_HEADERS}#include <cppkafka/${header}>\n")
        endif()
    endforeach()

    #create file from template
    configure_file(${PROJECT_SOURCE_DIR}/cppkafka.h.in ${CPPKAFKA_HEADER})
endfunction()

# Run file generation function
make_cppkafka_header()

# Install headers including the auto-generated cppkafka.h
file(GLOB INCLUDE_FILES "*.h")
file(GLOB UTILS_INCLUDE_FILES "utils/*.h")
file(GLOB DETAIL_INCLUDE_FILES "detail/*.h")
install(
    FILES ${INCLUDE_FILES}
    DESTINATION include/cppkafka
    COMPONENT Headers
)
install(
    FILES ${UTILS_INCLUDE_FILES}
    DESTINATION include/cppkafka/utils/
    COMPONENT Headers
)
install(
    FILES ${DETAIL_INCLUDE_FILES}
    DESTINATION include/cppkafka/detail/
    COMPONENT Headers
)
