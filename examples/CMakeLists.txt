find_package(Boost COMPONENTS program_options)

if (Boost_PROGRAM_OPTIONS_FOUND)
    link_libraries(${Boost_LIBRARIES} cppkafka ${RDKAFKA_LIBRARY})

    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
    include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${RDKAFKA_INCLUDE_DIR})

    add_custom_target(examples)
    macro(create_example example_name)
        add_executable(${example_name} EXCLUDE_FROM_ALL "${example_name}.cpp")
        add_dependencies(examples ${example_name})
    endmacro()

    create_example(kafka_producer)
    create_example(kafka_consumer)
    create_example(metadata)
    create_example(consumers_information)
else()
    message(STATUS "Disabling examples since boost.program_options was not found")
endif()
