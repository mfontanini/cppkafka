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
    metadata.cpp

    kafka_handle_base.cpp
    producer.cpp
    consumer.cpp
)

if (ENABLE_ZOOKEEPER)
    add_definitions("-DCPPKAFKA_ENABLE_ZOOKEEPER=1")
    set(ZOOKEEPER_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/zookeeper/zookeeper_watcher.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/zookeeper/zookeeper_pool.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/zookeeper/zookeeper_subscriber.cpp)
    # Add json library as include dir (header only)
    include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/json)
    set(SOURCES ${SOURCES} ${ZOOKEEPER_SOURCES})
endif()

add_library(cppkafka ${SOURCES})