cmake_minimum_required(VERSION 2.8.1)
project(cppkafka)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

# Set output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

include_directories(${CMAKE_SOURCE_DIR}/include/cppkafka)

# Look for Boost (just need boost.optional headers here)
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(RdKafka REQUIRED)

add_subdirectory(src)
add_subdirectory(include)

add_subdirectory(examples)

set(GOOGLETEST_ROOT ${CMAKE_SOURCE_DIR}/third_party/googletest)
if(EXISTS "${GOOGLETEST_ROOT}/CMakeLists.txt")
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

    enable_testing()
    add_subdirectory(tests)
    # Make sure we build googletest before anything else
    add_dependencies(cppkafka googletest)
else()
    message(STATUS "Disabling tests")
endif()
