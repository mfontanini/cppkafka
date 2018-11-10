set(SOURCES
    configuration.cpp
    topic_configuration.cpp
    configuration_option.cpp
    exceptions.cpp
    topic.cpp
    buffer.cpp
    queue.cpp
    message.cpp
    message_timestamp.cpp
    message_internal.cpp
    topic_partition.cpp
    topic_partition_list.cpp
    metadata.cpp
    group_information.cpp
    error.cpp
    event.cpp

    kafka_handle_base.cpp
    producer.cpp
    consumer.cpp

    utils/backoff_performer.cpp
    utils/backoff_committer.cpp
    utils/poll_strategy_base.cpp
    utils/roundrobin_poll_strategy.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include/cppkafka)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${RDKAFKA_INCLUDE_DIR})

add_library(cppkafka ${CPPKAFKA_LIBRARY_TYPE} ${SOURCES})
set_target_properties(cppkafka PROPERTIES VERSION ${CPPKAFKA_VERSION}
                                          SOVERSION ${CPPKAFKA_VERSION})

set(DEPENDENCIES ${RDKAFKA_LIBRARY})
if (WIN32)
    # On windows ntohs and related are in ws2_32
    set(DEPENDENCIES ${DEPENDENCIES} ws2_32.lib)
endif()
target_link_libraries(cppkafka ${DEPENDENCIES})
target_include_directories(cppkafka PUBLIC ${PROJECT_SOURCE_DIR}/include)

install( 
    TARGETS cppkafka
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT dev
)
