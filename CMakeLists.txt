cmake_minimum_required(VERSION 2.8.1)
project(cppkafka)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories(${CMAKE_SOURCE_DIR}/include/cppkafka)

add_subdirectory(src)