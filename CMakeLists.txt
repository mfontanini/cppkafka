cmake_minimum_required(VERSION 2.8.1)
project(cppkafka)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")

include_directories(${CMAKE_SOURCE_DIR}/include/cppkafka)

add_subdirectory(src)

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
# Make sure we build googletest before anything else
add_dependencies(cppkafka googletest)

option(ENABLE_ZOOKEEPER "Whether to enable zookeeper support" ON)

if (ENABLE_ZOOKEEPER)
    set(CPPKAFKA_HAVE_ZOOKEEPER ON)
endif()

# Configuration file
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/include/cppkafka/config.h.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/cppkafka/config.h"
)

enable_testing()
add_subdirectory(tests)