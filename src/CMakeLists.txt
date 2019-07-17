set(SOURCES
    configuration.cpp
    topic_configuration.cpp
    configuration_option.cpp
    exceptions.cpp
    topic.cpp
    buffer.cpp
    queue.cpp
    message.cpp
    message_timestamp.cpp
    message_internal.cpp
    topic_partition.cpp
    topic_partition_list.cpp
    metadata.cpp
    group_information.cpp
    error.cpp
    event.cpp

    kafka_handle_base.cpp
    producer.cpp
    consumer.cpp

    utils/backoff_performer.cpp
    utils/backoff_committer.cpp
    utils/poll_strategy_base.cpp
    utils/roundrobin_poll_strategy.cpp
)

set(TARGET_NAME         cppkafka)
set(PKG_DIR             "${CMAKE_BINARY_DIR}/package")
set(PKG_CONFIG_FILE     "${PKG_DIR}/${TARGET_NAME}.pc")
set(CONFIG_FILE         "${PKG_DIR}/${PROJECT_NAME}Config.cmake")
set(VERSION_FILE        "${PKG_DIR}/${PROJECT_NAME}ConfigVersion.cmake")
set(FIND_RDKAFKA_FILE   "${PROJECT_SOURCE_DIR}/cmake/FindRdKafka.cmake")
set(NAMESPACE           "${PROJECT_NAME}::")
set(TARGET_EXPORT_NAME  ${PROJECT_NAME}Targets)

add_library(${TARGET_NAME} ${CPPKAFKA_LIBRARY_TYPE} ${SOURCES})
target_include_directories(${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include/cppkafka>)
set_target_properties(${TARGET_NAME} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_INSTALL_LIBDIR}"
        ARCHIVE_OUTPUT_NAME "${TARGET_NAME}"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY_OUTPUT_NAME "${TARGET_NAME}"
        INSTALL_RPATH "${CMAKE_INSTALL_LIBDIR}"
        INSTALL_RPATH_USE_LINK_PATH TRUE
        VERSION ${CPPKAFKA_VERSION}
        SOVERSION ${CPPKAFKA_VERSION})
# In CMake >= 3.15 Boost::boost == Boost::headers
target_link_libraries(${TARGET_NAME} PUBLIC RdKafka::rdkafka Boost::boost)
if (WIN32)
    # On windows ntohs and related are in ws2_32
    target_link_libraries(${TARGET_NAME} PUBLIC ws2_32.lib)
endif()

# Install cppkafka target and specify all properties needed for the exported file
install(
    TARGETS ${TARGET_NAME}
    EXPORT ${TARGET_EXPORT_NAME}
    COMPONENT binaries
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

if (CPPKAFKA_EXPORT_PKGCONFIG)
    # Generate and install pkgconfig file
    configure_file(${PROJECT_SOURCE_DIR}/cmake/cppkafka.pc.in ${PKG_CONFIG_FILE} @ONLY)

    install(
        FILES ${PKG_CONFIG_FILE}
        DESTINATION "${CPPKAFKA_PKGCONFIG_DIR}"
        COMPONENT pkgconfig
    )
endif()

if (CPPKAFKA_EXPORT_CMAKE_CONFIG)
    # Install the exported file
    install(
        EXPORT "${TARGET_EXPORT_NAME}"
        NAMESPACE "${NAMESPACE}"
        COMPONENT config
        DESTINATION "${CPPKAFKA_CONFIG_DIR}"
    )

    # Generate CMAKE configuration file and exported targets
    configure_package_config_file(
        "${PROJECT_SOURCE_DIR}/cmake/config.cmake.in"
        "${CONFIG_FILE}"
        INSTALL_DESTINATION "${CPPKAFKA_CONFIG_DIR}"
        PATH_VARS RDKAFKA_MIN_VERSION_HEX CMAKE_INSTALL_PREFIX CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR
    )

    # Generate version file
    write_basic_package_version_file(
        "${VERSION_FILE}"
        VERSION ${CPPKAFKA_VERSION}
        COMPATIBILITY AnyNewerVersion
    )

    install(
        FILES "${CONFIG_FILE}" "${VERSION_FILE}" "${FIND_RDKAFKA_FILE}"
        DESTINATION "${CPPKAFKA_CONFIG_DIR}"
        COMPONENT config
    )
endif()
