set(SOURCES
    configuration.cpp
    topic_configuration.cpp
    exceptions.cpp
    topic.cpp
    partition.cpp

    kafka_handle_base.cpp
    producer.cpp
)

add_library(cppkafka ${SOURCES})