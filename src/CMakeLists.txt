set(SOURCES
    configuration.cpp
    topic_configuration.cpp
    configuration_option.cpp
    exceptions.cpp
    topic.cpp
    partition.cpp
    buffer.cpp
    message.cpp
    topic_partition.cpp
    topic_partition_list.cpp
    metadata.cpp
    error.cpp

    kafka_handle_base.cpp
    producer.cpp
    consumer.cpp
)

add_library(cppkafka ${CPPKAFKA_LIBRARY_TYPE} ${SOURCES})
set_target_properties(cppkafka PROPERTIES VERSION ${CPPKAFKA_VERSION}
                                          SOVERSION ${CPPKAFKA_VERSION})
target_link_libraries(cppkafka ${RDKAFKA_LIBRARY})

install( 
    TARGETS cppkafka
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT dev
)
