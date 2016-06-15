file(GLOB INCLUDE_FILES "*.h")
install(
    FILES ${INCLUDE_FILES} 
    DESTINATION include/cppkafka/zookeeper
    COMPONENT Headers
)
