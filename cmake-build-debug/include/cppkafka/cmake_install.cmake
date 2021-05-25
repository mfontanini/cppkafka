# Install script for directory: /home/illidansr/lib/cppkafka/include/cppkafka

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "0")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xHeadersx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/cppkafka" TYPE FILE FILES
    "/home/illidansr/lib/cppkafka/include/cppkafka/buffer.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/clonable_ptr.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/configuration.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/configuration_base.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/configuration_option.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/consumer.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/cppkafka.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/error.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/event.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/exceptions.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/group_information.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/header.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/header_list.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/header_list_iterator.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/kafka_handle_base.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/logging.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/macros.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/message.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/message_builder.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/message_internal.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/message_timestamp.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/metadata.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/producer.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/queue.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/topic.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/topic_configuration.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/topic_partition.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/topic_partition_list.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xHeadersx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/cppkafka/utils" TYPE FILE FILES
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/backoff_committer.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/backoff_performer.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/buffered_producer.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/compacted_topic_processor.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/consumer_dispatcher.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/poll_interface.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/poll_strategy_base.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/utils/roundrobin_poll_strategy.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xHeadersx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/cppkafka/detail" TYPE FILE FILES
    "/home/illidansr/lib/cppkafka/include/cppkafka/detail/callback_invoker.h"
    "/home/illidansr/lib/cppkafka/include/cppkafka/detail/endianness.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xHeadersx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/cppkafka" TYPE FILE FILES "/home/illidansr/lib/cppkafka/cmake-build-debug/include/cppkafka/cppkafka.h")
endif()

