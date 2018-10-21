include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include/)
include_directories(SYSTEM ${CATCH_INCLUDE})
include_directories(SYSTEM ${RDKAFKA_INCLUDE_DIR})

set(KAFKA_TEST_INSTANCE "kafka-vm:9092" 
    CACHE STRING "The kafka instance to which to connect to run tests")
add_custom_target(tests)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions("-DKAFKA_TEST_INSTANCE=\"${KAFKA_TEST_INSTANCE}\"")

add_executable(cppkafka_tests
    buffer_test.cpp
    compacted_topic_processor_test.cpp
    configuration_test.cpp
    topic_partition_list_test.cpp
    kafka_handle_base_test.cpp
    producer_test.cpp
    consumer_test.cpp
    roundrobin_poll_test.cpp
    headers_test.cpp
    test_utils.cpp

    # Main file
    test_main.cpp
)
target_link_libraries(cppkafka_tests cppkafka ${RDKAFKA_LIBRARY} pthread rt ssl crypto dl z)
add_dependencies(tests cppkafka_tests)
add_test(cppkafka cppkafka_tests)
