cmake_minimum_required(VERSION 2.8.1)
project(cppkafka)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

include_directories(${CMAKE_SOURCE_DIR}/include/cppkafka)

set(GOOGLETEST_ROOT ${CMAKE_SOURCE_DIR}/third_party/googletest)
set(GOOGLETEST_INCLUDE ${GOOGLETEST_ROOT}/googletest/include)
set(GOOGLETEST_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/googletest)
set(GOOGLETEST_LIBRARY ${GOOGLETEST_BINARY_DIR}/googletest)

include(ExternalProject)

ExternalProject_Add(
    googletest
    DOWNLOAD_COMMAND ""
    SOURCE_DIR ${GOOGLETEST_ROOT}
    BINARY_DIR ${GOOGLETEST_BINARY_DIR}
    CMAKE_CACHE_ARGS "-DBUILD_GTEST:bool=ON" "-DBUILD_GMOCK:bool=OFF"
                     "-Dgtest_force_shared_crt:bool=ON"
    INSTALL_COMMAND ""
)

# Look for Boost (just need boost.optional headers here)
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(RdKafka REQUIRED)

option(ENABLE_ZOOKEEPER "Whether to enable zookeeper support" ON)
if (ENABLE_ZOOKEEPER)
    find_package(Zookeeper REQUIRED)

    message(STATUS "Found zookeeper library")
    include_directories(${ZOOKEEPER_INCLUDE_DIR})

    set(CPPKAFKA_HAVE_ZOOKEEPER ON)
endif()

# Configuration file
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/include/cppkafka/config.h.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/cppkafka/config.h"
)

add_subdirectory(src)
add_subdirectory(include)
# Make sure we build googletest before anything else
add_dependencies(cppkafka googletest)
enable_testing()
add_subdirectory(tests)
add_subdirectory(examples)
