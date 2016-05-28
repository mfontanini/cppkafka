include_directories(${GOOGLETEST_INCLUDE})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include/)

link_directories(${GOOGLETEST_LIBRARY})
link_libraries(cppkafka rdkafka gtest gtest_main pthread)

set(KAFKA_TEST_INSTANCE "127.0.0.1:9092" 
    CACHE STRING "The kafka instance to which connect to run tests")

macro(create_test test_name)
    add_executable(${test_name}_test "${test_name}_test.cpp")
    add_test(${test_name} ${test_name}_test)
endmacro()

add_definitions("-DKAFKA_TEST_INSTANCE=\"${KAFKA_TEST_INSTANCE}\"")

create_test(producer)
create_test(kafka_handle_base)
create_test(topic_partition_list)
