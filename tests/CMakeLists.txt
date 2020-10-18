include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include/)
include_directories(SYSTEM ${CATCH_INCLUDE})

if (NOT KAFKA_TEST_INSTANCE)
set(KAFKA_TEST_INSTANCE kafka-vm:9092
    CACHE STRING "The kafka instance to which to connect to run tests")
endif()
if (NOT KAFKA_NUM_PARTITIONS)
   set(KAFKA_NUM_PARTITIONS 3 CACHE STRING "Kafka Number of partitions")
endif()
if (NOT KAFKA_TOPICS)
   set(KAFKA_TOPICS "cppkafka_test1;cppkafka_test2" CACHE STRING "Kafka topics")
endif()

# Convert list of topics into a C++ initializer list
FOREACH(TOPIC ${KAFKA_TOPICS})
   if (NOT TOPIC_LIST)
      set(TOPIC_LIST "\"${TOPIC}\"")
   else()
      set(TOPIC_LIST "${TOPIC_LIST},\"${TOPIC}\"")
   endif()
ENDFOREACH()

add_custom_target(tests)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(
  "-DKAFKA_TEST_INSTANCE=\"${KAFKA_TEST_INSTANCE}\""
  -DKAFKA_NUM_PARTITIONS=${KAFKA_NUM_PARTITIONS}
  -DKAFKA_TOPIC_NAMES=${TOPIC_LIST}
)

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

# In CMake >= 3.15 Boost::boost == Boost::headers
target_link_libraries(cppkafka_tests cppkafka RdKafka::rdkafka Boost::boost)
add_dependencies(tests cppkafka_tests)
add_test(cppkafka cppkafka_tests)
