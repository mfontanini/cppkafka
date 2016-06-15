find_package(Boost COMPONENTS program_options)

if (Boost_PROGRAM_OPTIONS_FOUND)
    link_libraries(${Boost_LIBRARIES} cppkafka ${RDKAFKA_LIBRARY} ${ZOOKEEPER_LIBRARY})

    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)

    add_executable(kafka_producer kafka_producer.cpp)
    add_executable(kafka_consumer kafka_consumer.cpp)
    add_custom_target(examples DEPENDS kafka_producer kafka_consumer)
else()
    message(STATUS "Disabling examples since boost.program_options was not found")
endif()
