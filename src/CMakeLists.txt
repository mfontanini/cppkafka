set(SOURCES
    configuration.cpp
    topic_configuration.cpp
    exceptions.cpp
    topic.cpp
    partition.cpp
    buffer.cpp
    message.cpp
    topic_partition.cpp
    topic_partition_list.cpp

    kafka_handle_base.cpp
    producer.cpp
    consumer.cpp
)

add_library(cppkafka ${SOURCES})