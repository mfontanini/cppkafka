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

    kafka_handle_base.cpp
    producer.cpp
    consumer.cpp
)

add_library(cppkafka ${CPPKAFKA_LIBRARY_TYPE} ${SOURCES})
target_link_libraries(cppkafka ${RDKAFKA_LIBRARY})

install( 
    TARGETS cppkafka
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT dev
)
