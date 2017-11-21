cmake_minimum_required(VERSION 2.8.1)
project(cppkafka)

# Set the version number.
set(CPPKAFKA_VERSION_MAJOR 0)
set(CPPKAFKA_VERSION_MINOR 1)
set(CPPKAFKA_VERSION "${CPPKAFKA_VERSION_MAJOR}.${CPPKAFKA_VERSION_MINOR}")

if(MSVC)
    # Don't always use Wall, since VC's /Wall is ridiculously verbose.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3")

    # Disable VC secure checks, since these are not really issues
    add_definitions("-D_CRT_SECURE_NO_WARNINGS=1")
    add_definitions("-D_SCL_SECURE_NO_WARNINGS=1")
    add_definitions("-DNOGDI=1")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")
endif()
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

# Set output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

# Build output checks
option(CPPKAFKA_BUILD_SHARED "Build cppkafka as a shared library." ON)
if(CPPKAFKA_BUILD_SHARED)
    message(STATUS "Build will generate a shared library. "
            "Use CPPKAFKA_BUILD_SHARED=0 to perform a static build")
    set(CPPKAFKA_LIBRARY_TYPE SHARED)
else()
    message(STATUS "Build will generate a static library.")
    set(CPPKAFKA_LIBRARY_TYPE STATIC)
    add_definitions("-DCPPKAFKA_STATIC=1")
endif()

# Look for Boost (just need boost.optional headers here)
find_package(Boost REQUIRED)
find_package(RdKafka REQUIRED)

add_subdirectory(src)
add_subdirectory(include)

add_subdirectory(examples)

# Add a target to generate API documentation using Doxygen
find_package(Doxygen QUIET)
if(DOXYGEN_FOUND)
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in 
        ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile 
        @ONLY
    )
    add_custom_target(
        docs
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
    )
endif(DOXYGEN_FOUND)

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

if(NOT TARGET uninstall)
    # Confiugure the uninstall script
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
        IMMEDIATE @ONLY
    )

    # Add uninstall target
    add_custom_target(uninstall
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif()
