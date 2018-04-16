include_directories(${GOOGLETEST_INCLUDE})
include_directories(SYSTEM ${CATCH_INCLUDE})
message(STATUS ${CATCH_INCLUDE})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include/)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${RDKAFKA_INCLUDE_DIR})

link_directories(${GOOGLETEST_LIBRARY})

set(KAFKA_TEST_INSTANCE "kafka-vm:9092" 
    CACHE STRING "The kafka instance to which to connect to run tests")
add_custom_target(tests)

macro(create_test test_name)
    add_executable(${test_name}_test EXCLUDE_FROM_ALL "${test_name}_test.cpp")
    add_test(${test_name} ${test_name}_test)
    add_dependencies(tests ${test_name}_test)
    add_dependencies(${test_name}_test cppkafka)
    target_link_libraries(${test_name}_test cppkafka-test gtest gtest_main pthread)
endmacro()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_library(cppkafka-test EXCLUDE_FROM_ALL test_utils.cpp)
target_link_libraries(cppkafka-test cppkafka ${RDKAFKA_LIBRARY})

add_definitions("-DKAFKA_TEST_INSTANCE=\"${KAFKA_TEST_INSTANCE}\"")
create_test(consumer)
create_test(producer)
create_test(kafka_handle_base)
create_test(topic_partition_list)
create_test(configuration)
create_test(compacted_topic_processor)

add_executable(
    catch_tests
    EXCLUDE_FROM_ALL
    buffer_test.cpp

    # Main file
    test_main.cpp
)
target_link_libraries(catch_tests cppkafka-test)
