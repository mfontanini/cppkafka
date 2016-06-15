find_package(Boost REQUIRED COMPONENTS program_options)

link_libraries(${Boost_LIBRARIES} cppkafka ${RDKAFKA_LIBRARY} ${ZOOKEEPER_LIBRARY})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)

add_executable(kafka_producer kafka_producer.cpp)
add_executable(kafka_consumer kafka_consumer.cpp)
