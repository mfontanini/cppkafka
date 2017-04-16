find_package(Boost COMPONENTS program_options)

if (Boost_PROGRAM_OPTIONS_FOUND)
    link_libraries(${Boost_LIBRARIES} cppkafka ${RDKAFKA_LIBRARY})

    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
    include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${RDKAFKA_INCLUDE_DIR})

    add_executable(kafka_producer EXCLUDE_FROM_ALL kafka_producer.cpp)
    add_executable(kafka_consumer EXCLUDE_FROM_ALL kafka_consumer.cpp)
    add_custom_target(examples DEPENDS kafka_producer kafka_consumer)
else()
    message(STATUS "Disabling examples since boost.program_options was not found")
endif()
