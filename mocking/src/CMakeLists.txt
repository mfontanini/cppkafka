set(SOURCES
    configuration_mock.cpp
    topic_mock.cpp
    partition_mock.cpp
    message_mock.cpp
    kafka_cluster.cpp
    kafka_cluster_registry.cpp
    api.cpp
)

include_directories(${PROJECT_SOURCE_DIR}/mocking/include/
                    ${PROJECT_SOURCE_DIR}/include/)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${RDKAFKA_INCLUDE_DIR})

add_library(cppkafka_mock EXCLUDE_FROM_ALL ${CPPKAFKA_LIBRARY_TYPE} ${SOURCES})
set_target_properties(cppkafka_mock PROPERTIES VERSION ${CPPKAFKA_VERSION}
                                               SOVERSION ${CPPKAFKA_VERSION})
target_link_libraries(cppkafka_mock ${RDKAFKA_LIBRARY})
