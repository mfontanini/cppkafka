file(GLOB INCLUDE_FILES "*.h")
file(GLOB UTILS_INCLUDE_FILES "utils/*.h")
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