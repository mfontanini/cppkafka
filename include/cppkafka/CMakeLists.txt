file(GLOB INCLUDE_FILES "*.h")
file(GLOB INCLUDE_FILES_IMPL "impl/*.h")
file(GLOB UTILS_INCLUDE_FILES "utils/*.h")
file(GLOB UTILS_INCLUDE_FILES_IMPL "utils/impl/*.h")
install(
    FILES ${INCLUDE_FILES}
    DESTINATION include/cppkafka
    COMPONENT Headers
)
install(
    FILES ${INCLUDE_FILES_IMPL}
    DESTINATION include/cppkafka/impl
    COMPONENT Headers
)
install(
    FILES ${UTILS_INCLUDE_FILES}
    DESTINATION include/cppkafka/utils/
    COMPONENT Headers
)
install(
    FILES ${UTILS_INCLUDE_FILES_IMPL}
    DESTINATION include/cppkafka/utils/impl
    COMPONENT Headers
)
